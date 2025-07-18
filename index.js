require("dotenv").config();

const fs = require("fs");
const path = require("path");
const util = require("util");
const archiver = require("archiver");
const AWS = require("aws-sdk");
const pino = require("pino");

const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);

const transport = pino.transport({
  targets: [{ target: "pino-pretty" }, { target: "pino/file", options: { destination: "backupper.log" } }]
});
const logger = pino(transport);

const config = JSON.parse(fs.readFileSync(path.join(__dirname, "/data/config.json"), "utf-8"));

const s3Config = {
  endpoint: process.env.ENDPOINT,
  region: process.env.REGION,
  accessKeyId: process.env.ACCESS_KEY,
  secretAccessKey: process.env.SECRET_KEY,
  s3ForcePathStyle: process.env.FORCE_PATH_STYLE === "true"
};
const s3 = new AWS.S3(s3Config);

async function zipDirectory(sourceDir, outPath) {
  const output = fs.createWriteStream(outPath);
  const archive = archiver("zip", { zlib: { level: 9 } });
  return new Promise((resolve, reject) => {
    output.on("close", () => resolve(archive.pointer()));
    archive.on("error", (err) => reject(err));
    archive.pipe(output);
    archive.directory(sourceDir, false);
    archive.finalize();
  });
}

async function listAllVersions(bucket, prefix) {
  let KeyMarker, VersionIdMarker;
  const versions = [];
  do {
    const res = await s3
      .listObjectVersions({
        Bucket: bucket,
        Prefix: prefix,
        KeyMarker,
        VersionIdMarker
      })
      .promise();
    if (res.Versions) versions.push(...res.Versions);
    KeyMarker = res.IsTruncated ? res.NextKeyMarker : null;
    VersionIdMarker = res.IsTruncated ? res.NextVersionIdMarker : null;
  } while (KeyMarker || VersionIdMarker);
  return versions;
}

async function enforceRetention(bucket, prefix, maxBackups) {
  const allVersions = await listAllVersions(bucket, prefix);
  const fileVersions = allVersions.filter((v) => {
    if (!v.Key.startsWith(`${prefix}/`) || !v.VersionId || v.IsDeleteMarker) return false;
    const rest = v.Key.slice(prefix.length + 1);
    return rest && rest.indexOf("/") === -1;
  });
  if (fileVersions.length <= maxBackups) return;
  fileVersions.sort((a, b) => new Date(a.LastModified) - new Date(b.LastModified));
  const toDelete = fileVersions.slice(0, fileVersions.length - maxBackups);
  logger.info(`toDelete length ${toDelete.length}`);
  for (let i = 0; i < toDelete.length; i += 1000) {
    const chunk = toDelete.slice(i, i + 1000);
    const delParams = {
      Bucket: bucket,
      Delete: {
        Objects: chunk.map((item) => ({ Key: item.Key, VersionId: item.VersionId }))
      }
    };
    const res = await s3.deleteObjects(delParams).promise();
    if (res.Errors && res.Errors.length) {
      logger.error("Errors deleting some object versions:", res.Errors);
    }
  }
}

async function getLatestBackupSize(bucket, prefix) {
  const params = { Bucket: bucket, Prefix: prefix };
  const listed = await s3.listObjectsV2(params).promise();
  if (!listed.Contents.length) return 0;
  const latest = listed.Contents.reduce((max, item) => (item.LastModified > max.LastModified ? item : max));
  return latest.Size;
}

async function backup() {
  for (const entry of config.backups) {
    const { directory: src, bucket, prefix, maxBackupCount = 7, sizeCheck = false, isZip = false } = entry;
    logger.info("---");
    logger.info(`Processing ${isZip ? "zip-dir" : "dir"} ${src}`);
    let zipPath;
    let newSize;
    let isTemp = false;
    if (isZip) {
      const files = await readdir(src);
      const zips = files.filter((f) => f.toLowerCase().endsWith(".zip"));
      if (!zips.length) {
        logger.error(`No .zip files found in ${src}, skipping.`);
        continue;
      }
      let latestFile = null;
      let latestMtime = 0;
      for (const f of zips) {
        const full = path.join(src, f);
        const stats = await stat(full);
        if (stats.mtimeMs > latestMtime) {
          latestMtime = stats.mtimeMs;
          latestFile = full;
        }
      }
      zipPath = latestFile;
      newSize = (await stat(zipPath)).size;
      logger.info(`Using latest zip ${zipPath} (${newSize} bytes).`);
    } else {
      const timestamp = new Date().toISOString().replace(/[:\.]/g, "-");
      const filename = `${path.basename(src)}-${timestamp}.zip`;
      zipPath = path.join(require("os").tmpdir(), filename);
      logger.info(`Zipping ${src} -> ${zipPath}...`);
      newSize = await zipDirectory(src, zipPath);
      logger.info(`Zipped ${newSize} bytes.`);
      isTemp = true;
    }
    if (sizeCheck) {
      logger.info("size check");
      const prevSize = await getLatestBackupSize(bucket, prefix);
      if (newSize <= prevSize) {
        logger.info(`Skip upload: new (${newSize}) <= prev (${prevSize}).`);
        if (isTemp) fs.unlinkSync(zipPath);
        continue;
      }
    }
    logger.info(`Enforcing retention (keep ${maxBackupCount})...`);
    // -1 to make way for current upload
    await enforceRetention(bucket, prefix, maxBackupCount - 1);
    const key = `${prefix}/${path.basename(zipPath)}`;
    logger.info(`Uploading to s3://${bucket}/${key}...`);
    await s3.upload({ Bucket: bucket, Key: key, Body: fs.createReadStream(zipPath) }).promise();
    logger.info(`Upload complete.`);
    if (isTemp) {
      fs.unlinkSync(zipPath);
      logger.info(`Removed temp file.`);
    }
  }
}

backup().catch((err) => {
  logger.error("Backup failed:", err);
  process.exit(1);
});
