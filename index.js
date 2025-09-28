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

async function enforceRetention(bucket, prefix, maxBackups, log) {
  const allVersions = await listAllVersions(bucket, prefix);
  const fileVersions = allVersions.filter((v) => {
    if (!v.Key.startsWith(`${prefix}/`) || !v.VersionId || v.IsDeleteMarker) return false;
    const rest = v.Key.slice(prefix.length + 1);
    return rest && !rest.includes("/");
  });

  if (fileVersions.length <= maxBackups) return;

  fileVersions.sort((a, b) => new Date(a.LastModified) - new Date(b.LastModified));
  const toDelete = fileVersions.slice(0, fileVersions.length - maxBackups);
  log.info(`toDelete length ${toDelete.length}`);

  for (let i = 0; i < toDelete.length; i += 1000) {
    const chunk = toDelete.slice(i, i + 1000);
    const res = await s3
      .deleteObjects({
        Bucket: bucket,
        Delete: { Objects: chunk.map((item) => ({ Key: item.Key, VersionId: item.VersionId })) }
      })
      .promise();

    if (res.Errors && res.Errors.length) {
      log.error("Errors deleting some object versions:", res.Errors);
    }
  }
}

async function getLatestBackupSize(bucket, prefix) {
  const listed = await s3.listObjectsV2({ Bucket: bucket, Prefix: prefix }).promise();
  if (!listed.Contents.length) return 0;

  const latest = listed.Contents.reduce((max, item) => (item.LastModified > max.LastModified ? item : max));
  return latest.Size;
}

async function checkSource(srcPath, log) {
  log.info("checkSource src: " + srcPath);
  try {
    const stats = await stat(srcPath);
    if (stats.isDirectory()) {
      const files = await readdir(srcPath);
      if (!files.length) {
        log.info(`${srcPath} empty`);
        return false;
      }
    } else if (stats.size === 0) {
      log.info(`${srcPath} empty`);
      return false;
    }
  } catch (error) {
    if (error.code !== "ENOENT") log.warn(error);
    return false;
  }
  return true;
}

// ---------- backup logic ----------
async function prepareBackupSource(src, isZip, log) {
  return isZip ? useExistingZip(src, log) : createNewZip(src, log);
}

async function useExistingZip(src, log) {
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
  log.info(`Using latest zip ${latestFile} (${size} bytes).`);
  return { zipPath: latestFile, newSize: size, isTemp: false };
}

async function createNewZip(src, log) {
  const timestamp = new Date().toISOString().replace(/[:\.]/g, "-");
  const filename = `${path.basename(src)}-${timestamp}.zip`;
  const zipPath = path.join(require("os").tmpdir(), filename);

  log.info(`Zipping ${src} -> ${zipPath}...`);
  const size = await zipDirectory(src, zipPath);
  log.info(`Zipped ${size} bytes.`);

  return { zipPath, newSize: size, isTemp: true };
}

async function shouldUpload(bucket, prefix, newSize, log) {
  log.info("size check");
  const prevSize = await getLatestBackupSize(bucket, prefix);
  if (newSize <= prevSize) {
    log.info(`Skip upload: new (${newSize}) <= prev (${prevSize}).`);
    return false;
  }
  return true;
}

async function uploadBackup(bucket, prefix, zipPath, log) {
  const key = `${prefix}/${path.basename(zipPath)}`;
  log.info(`Uploading to s3://${bucket}/${key}...`);

  await s3
    .upload({
      Bucket: bucket,
      Key: key,
      Body: fs.createReadStream(zipPath)
    })
    .promise();

  log.info("Upload complete prefix: " + prefix);
}

// ---------- main entry ----------
async function backup(entry) {
  const { src, bucket, prefix, maxBackupCount = 7, sizeCheck = false, isZip = false, jobName } = entry;
  const log = logger.child({ jobName });

  log.info(`Start Processing ${isZip ? "zip-dir" : "dir"} ${src}`);

  if (!(await checkSource(src, log))) {
    log.info(`Skipping ${src}`);
    return;
  }

  const { zipPath, newSize, isTemp } = await prepareBackupSource(src, isZip, log);

  try {
    if (sizeCheck && !(await shouldUpload(bucket, prefix, newSize, log))) return;

    await enforceRetention(bucket, prefix, maxBackupCount - 1, log);
    await uploadBackup(bucket, prefix, zipPath, log);
  } catch (error) {
    log.error(error.message);
    throw error;
  } finally {
    if (isTemp && fs.existsSync(zipPath)) {
      fs.unlinkSync(zipPath);
      log.info("Removed temp file.");
    }
  }
}

// ---------- scheduler ----------
async function start() {
  for (const entry of config.backups) {
    const { schedule, jobName } = entry;
    const log = logger.child({ jobName });

    if (!cron.validate(schedule)) {
      log.warn(`Invalid cron expression for "${jobName}": ${schedule}`);
      continue;
    }

    cron.schedule(schedule, async () => {
      try {
        await backup(entry);
      } catch (error) {
        log.error(`Scheduled "${jobName}" failed`);
        log.error(error);
      }
    });

    log.info(`Scheduled "${jobName}" with cron: ${schedule}`);
  }
}

start().catch((error) => {
  logger.error("Backup failed:", error);
});
