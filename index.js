require("dotenv").config();

const fs = require("fs");
const path = require("path");
const util = require("util");
const archiver = require("archiver");
const AWS = require("aws-sdk");
const pino = require("pino");
const cron = require("node-cron");

const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);

const transport = pino.transport({
  targets: [{ target: "pino-pretty" }, { target: "pino/file", options: { destination: "backupper.log" } }]
});
const logger = pino(transport);

const config = JSON.parse(fs.readFileSync(path.join(__dirname, "/data/config.json"), "utf-8"));

const s3 = new AWS.S3({
  endpoint: process.env.ENDPOINT,
  region: process.env.REGION,
  accessKeyId: process.env.ACCESS_KEY,
  secretAccessKey: process.env.SECRET_KEY,
  s3ForcePathStyle: process.env.FORCE_PATH_STYLE === "true"
});

// ---------- helpers ----------
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
    return rest && !rest.includes("/");
  });

  if (fileVersions.length <= maxBackups) return;

  fileVersions.sort((a, b) => new Date(a.LastModified) - new Date(b.LastModified));
  const toDelete = fileVersions.slice(0, fileVersions.length - maxBackups);
  logger.info(`toDelete length ${toDelete.length}`);

  for (let i = 0; i < toDelete.length; i += 1000) {
    const chunk = toDelete.slice(i, i + 1000);
    const res = await s3
      .deleteObjects({
        Bucket: bucket,
        Delete: { Objects: chunk.map((item) => ({ Key: item.Key, VersionId: item.VersionId })) }
      })
      .promise();

    if (res.Errors && res.Errors.length) {
      logger.error("Errors deleting some object versions:", res.Errors);
    }
  }
}

async function getLatestBackupSize(bucket, prefix) {
  const listed = await s3.listObjectsV2({ Bucket: bucket, Prefix: prefix }).promise();
  if (!listed.Contents.length) return 0;

  const latest = listed.Contents.reduce((max, item) => (item.LastModified > max.LastModified ? item : max));
  return latest.Size;
}

async function checkSource(srcPath) {
  logger.info("checkSource", srcPath);
  try {
    const stats = await stat(srcPath);
    if (stats.isDirectory()) {
      const files = await readdir(srcPath);
      if (!files.length) {
        logger.info(`${srcPath} empty`);
        return false;
      }
    } else if (stats.size === 0) {
      logger.info(`${srcPath} empty`);
      return false;
    }
  } catch (error) {
    if (error.code !== "ENOENT") logger.warn(error);
    return false;
  }
  return true;
}

// ---------- backup logic ----------
async function prepareBackupSource(src, isZip) {
  return isZip ? useExistingZip(src) : createNewZip(src);
}

async function useExistingZip(src) {
  const files = await readdir(src);
  const zips = files.filter((f) => f.toLowerCase().endsWith(".zip"));
  if (!zips.length) {
    throw new Error(`No .zip files found in ${src}`);
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

  const size = (await stat(latestFile)).size;
  logger.info(`Using latest zip ${latestFile} (${size} bytes).`);
  return { zipPath: latestFile, newSize: size, isTemp: false };
}

async function createNewZip(src) {
  const timestamp = new Date().toISOString().replace(/[:\.]/g, "-");
  const filename = `${path.basename(src)}-${timestamp}.zip`;
  const zipPath = path.join(require("os").tmpdir(), filename);

  logger.info(`Zipping ${src} -> ${zipPath}...`);
  const size = await zipDirectory(src, zipPath);
  logger.info(`Zipped ${size} bytes.`);

  return { zipPath, newSize: size, isTemp: true };
}

async function shouldUpload(bucket, prefix, newSize) {
  logger.info("size check");
  const prevSize = await getLatestBackupSize(bucket, prefix);
  if (newSize <= prevSize) {
    logger.info(`Skip upload: new (${newSize}) <= prev (${prevSize}).`);
    return false;
  }
  return true;
}

async function uploadBackup(bucket, prefix, zipPath) {
  const key = `${prefix}/${path.basename(zipPath)}`;
  logger.info(`Uploading to s3://${bucket}/${key}...`);

  await s3
    .upload({
      Bucket: bucket,
      Key: key,
      Body: fs.createReadStream(zipPath)
    })
    .promise();

  logger.info("Upload complete.");
}

// ---------- main entry ----------
async function backup(entry) {
  const { src, bucket, prefix, maxBackupCount = 7, sizeCheck = false, isZip = false } = entry;

  logger.info("---");
  logger.info(`Processing ${isZip ? "zip-dir" : "dir"} ${src}`);

  if (!(await checkSource(src))) {
    logger.info(`Skipping ${src}`);
    return;
  }

  const { zipPath, newSize, isTemp } = await prepareBackupSource(src, isZip);

  try {
    if (sizeCheck && !(await shouldUpload(bucket, prefix, newSize))) return;

    await enforceRetention(bucket, prefix, maxBackupCount - 1);
    await uploadBackup(bucket, prefix, zipPath);
  } catch (err) {
    logger.error(err.message);
    throw err;
  } finally {
    if (isTemp && fs.existsSync(zipPath)) {
      fs.unlinkSync(zipPath);
      logger.info("Removed temp file.");
    }
  }
}

// ---------- scheduler ----------
async function start() {
  for (const entry of config.backups) {
    const { schedule, jobName } = entry;

    if (!cron.validate(schedule)) {
      logger.warn(`Invalid cron expression for "${jobName}": ${schedule}`);
      continue;
    }

    cron.schedule(schedule, async () => {
      try {
        await backup(entry);
      } catch (error) {
        logger.error(`Scheduled "${jobName}" failed`);
        logger.error(error);
      }
    });

    logger.info(`Scheduled "${jobName}" with cron: ${schedule}`);
  }
}

start().catch((error) => {
  logger.error("Backup failed:", error);
});
