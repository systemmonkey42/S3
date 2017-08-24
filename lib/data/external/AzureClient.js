const crypto = require('crypto');

const { errors, s3middleware } = require('arsenal');
const azure = require('azure-storage');
const createLogger = require('../multipleBackendLogger');
const logHelper = require('./utils').logHelper;
const azureMpuUtils = s3middleware.azureMpuUtils;
const objectUtils = s3middleware.objectUtils;
const ResultsCollector = s3middleware.ResultsCollector;
const SubStreamInterface = s3middleware.SubStreamInterface;
const MD5Sum = s3middleware.MD5Sum;

class AzureClient {
    constructor(config) {
        this._azureBlobEndpoint = config.azureBlobEndpoint;
        this._azureBlobSAS = config.azureBlobSAS;
        this._azureContainerName = config.azureContainerName;
        this._client = azure.createBlobServiceWithSas(
          this._azureBlobEndpoint, this._azureBlobSAS);
        this._dataStoreName = config.dataStoreName;
        this._bucketMatch = config.bucketMatch;
    }

    _createAzureKey(requestBucketName, requestObjectKey,
        bucketMatch) {
        if (bucketMatch) {
            return requestObjectKey;
        }
        return `${requestBucketName}/${requestObjectKey}`;
    }

    _translateMetaHeaders(metaHeaders, tags) {
        const translatedMetaHeaders = {};
        if (tags) {
            // tags are passed as string of format 'key1=value1&key2=value2'
            const tagObj = {};
            const tagArr = tags.split('&');
            tagArr.forEach(keypair => {
                const equalIndex = keypair.indexOf('=');
                const key = keypair.substring(0, equalIndex);
                tagObj[key] = keypair.substring(equalIndex + 1);
            });
            translatedMetaHeaders.tags = JSON.stringify(tagObj);
        }
        Object.keys(metaHeaders).forEach(headerName => {
            const translated = headerName.replace(/-/g, '_');
            translatedMetaHeaders[translated] = metaHeaders[headerName];
        });
        return translatedMetaHeaders;
    }

    _getMetaHeaders(objectMD) {
        const metaHeaders = {};
        Object.keys(objectMD).forEach(mdKey => {
            const isMetaHeader = mdKey.startsWith('x-amz-meta-');
            if (isMetaHeader) {
                metaHeaders[mdKey] = objectMD[mdKey];
            }
        });
        return this._translateMetaHeaders(metaHeaders);
    }

    _putSummaryPart(objInfo, log, cb) {
        const summaryPartId = azureMpuUtils.getSummaryPartId(objInfo.partNumber,
            objInfo.eTag, objInfo.size);
        const mpuSummaryKey =
            azureMpuUtils.getMpuSummaryKey(objInfo.azureKey, objInfo.uploadId);

        // NOTE: hacky trick to get the size of the summary part
        // to reflect the number of subparts (max should be around 50).
        // We put 1 byte if there are no subparts (a 0-byte part) since Azure
        // does not accept putting blocks of 0 bytes.
        const content = objInfo.numberSubParts === 0 ? Buffer.alloc(1) :
            Buffer.alloc(objInfo.numberSubParts);
        return this._client.createBlockFromText(summaryPartId,
        this._azureContainerName, mpuSummaryKey, content, {},
        err => {
            if (err) {
                logHelper(log, 'error', 'err from Azure data backend ' +
                    'uploadPart', err, this._dataStoreName);
                return cb(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `Azure: ${err.message}`)
                );
            }
            const dataRetrievalInfo = {
                key: objInfo.key,
                mpuSummaryId: summaryPartId,
                mpuSummaryKey,
                dataStoreName: this._dataStoreName,
            };
            return cb(null, dataRetrievalInfo);
        });
    }

    _putNextSubPart(partParams, subPartInfo, subPartStream, subPartIndex,
        resultsCollector) {
        const { uploadId, partNumber, bucketName, objectKey } = partParams;
        const subPartSize =
            azureMpuUtils.getSubPartSize(subPartInfo, subPartIndex);
        const subPartId =
            azureMpuUtils.getBlockId(uploadId, partNumber, subPartIndex);
        resultsCollector.pushOp();
        return this._client.createBlockFromStream(subPartId, bucketName,
            objectKey, subPartStream, subPartSize, {}, err => {
                resultsCollector.pushResult(err, subPartIndex);
            });
    }

    _putSubParts(request, params, objInfo, log, cb) {
        log.trace('data length is greater than max subpart size;' +
            'putting multiple parts');
        const subPartInfo = azureMpuUtils.getSubPartInfo(params.size);
        const resultsCollector = new ResultsCollector();
        const hashedStream = new MD5Sum();
        const streamInterface = new SubStreamInterface(hashedStream);

        resultsCollector.on('error', (err, subPartIndex) => {
            streamInterface.stopStreaming(request);
            logHelper(log, 'error', 'err from Azure data backend ' +
                `putting subpart ${subPartIndex}`, err, this._dataStoreName);
            return cb(errors.InternalError
                .customizeDescription('Error returned from ' +
                `Azure: ${err.message}`)
            );
        });
        resultsCollector.on('done', (err, results) => {
            if (err) {
                logHelper(log, 'error', 'err from Azure data backend ' +
                    'putting last subpart', err, this._dataStoreName);
                return cb(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `Azure: ${err.message}`)
                );
            }
            const numberSubParts = results.length;
            const totalLength = streamInterface.getTotalBytesStreamed();
            log.trace('successfully put subparts to Azure',
                { numberSubParts, totalLength });
            /* eslint-disable no-param-reassign */
            objInfo.numberSubParts = numberSubParts;
            objInfo.size = totalLength;
            hashedStream.on('hashed', () => {
                log.trace('hashed event emitted');
                objInfo.eTag = hashedStream.completedHash;
                return this._putSummaryPart(objInfo, log, cb);
            });
            // in case the hashed event was already emitted before the
            // event handler was registered:
            if (hashedStream.completedHash) {
                hashedStream.removeAllListeners('hashed');
                objInfo.eTag = hashedStream.completedHash;
                return this._putSummaryPart(objInfo, log, cb);
            }
            return undefined;
        });

        const currentStream = streamInterface.getCurrentStream();
        // start first put to Azure before we start streaming the data
        this._putNextSubPart(params, subPartInfo, currentStream, 0,
        resultsCollector);

        request.pipe(hashedStream);
        hashedStream.on('end', () => {
            resultsCollector.enableComplete();
            streamInterface.endStreaming();
        });
        hashedStream.on('data', data => {
            const currentLength = streamInterface.getLengthCounter();
            if (currentLength + data.length > azureMpuUtils.maxSubPartSize) {
                const { nextStream, subPartIndex } =
                    streamInterface.transitionToNextStream();
                this._putNextSubPart(params, subPartInfo, nextStream,
                subPartIndex, resultsCollector);
            }
            streamInterface.write(data);
        });
    }

    put(stream, size, keyContext, reqUids, callback) {
        const azureKey = this._createAzureKey(keyContext.bucketName,
            keyContext.objectKey, this._bucketMatch);
        const options = { metadata:
            this._translateMetaHeaders(keyContext.metaHeaders,
                keyContext.tagging) };
        this._client.createBlockBlobFromStream(this._azureContainerName,
          azureKey, stream, size, options, err => {
              if (err) {
                  const log = createLogger(reqUids);
                  logHelper(log, 'error', 'err from Azure PUT data backend',
                    err, this._dataStoreName);
                  return callback(errors.InternalError
                    .customizeDescription('Error returned from ' +
                    `Azure: ${err.message}`)
                  );
              }
              return callback(null, azureKey);
          });
    }

    get(objectGetInfo, range, reqUids, callback) {
        // for backwards compatibility
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
          objectGetInfo.key;
        const response = objectGetInfo.response;
        const azureStreamingOptions = objectGetInfo.azureStreamingOptions;
        this._client.getBlobToStream(this._azureContainerName, key, response,
          azureStreamingOptions, err => {
              if (err) {
                  const log = createLogger(reqUids);
                  logHelper(log, 'error', 'err from Azure GET data backend',
                    err, this._dataStoreName);
                  return callback(errors.InternalError);
              }
              return callback();
          });
    }

    delete(objectGetInfo, reqUids, callback) {
        // for backwards compatibility
        const key = typeof(objectGetInfo) === 'string' ? objectGetInfo :
          objectGetInfo.key;
        return this._client.deleteBlob(this._azureContainerName, key,
        err => {
            if (err) {
                const log = createLogger(reqUids);
                logHelper(log, 'error', 'error deleting object from ' +
                  'Azure datastore', err, this._dataStoreName);
                return callback(errors.InternalError
                  .customizeDescription('Error returned from ' +
                  `Azure: ${err.message}`));
            }
            return callback();
        });
    }

    checkAzureHealth(location, callback) {
        const azureResp = {};
        this._client.doesContainerExist(this._azureContainerName, err => {
            /* eslint-disable no-param-reassign */
            if (err) {
                azureResp[location] = { error: err.message };
                return callback(null, azureResp);
            }
            azureResp[location] = {
                message: 'Congrats! You own the azure container',
            };
            return callback(null, azureResp);
        });
    }

    uploadPart(request, streamingV4Params, stream, size, key, uploadId,
    partNumber, bucket, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const contentMD5 = request.headers['content-md5'];
        const zeroByteETag = crypto.createHash('md5').update('').digest('hex');
        const objectInfo = {
            key,
            azureKey,
            size,
            partNumber,
            uploadId,
        };

        if (size === 0) {
            // Azure does not allow putting empty blocks; instead we
            // will just upload a summary part with content-length of 0.
            objectInfo.eTag = zeroByteETag;
            objectInfo.numberSubParts = 0;
            return this._putSummaryPart(objectInfo, log, callback);
        }
        if (size <= azureMpuUtils.maxSubPartSize) {
            const blockId = azureMpuUtils.getBlockId(uploadId, partNumber, 0);
            const passThrough = new stream.PassThrough();
            const options = {};
            if (contentMD5) {
                options.useTransactionalMD5 = true;
                options.transactionalContentMD5 =
                    objectUtils.getBase64MD5(contentMD5);
            }
            request.pipe(passThrough);
            return this._client.createBlockFromStream(blockId,
            this._azureContainerName, azureKey, passThrough, size, options,
            (err, result) => {
                if (err) {
                    logHelper(log, 'error', 'err from Azure data backend ' +
                        'uploadPart', err, this._dataStoreName);
                    return callback(errors.InternalError
                        .customizeDescription('Error returned from ' +
                        `Azure: ${err.message}`)
                    );
                }
                const eTag = objectUtils.getHexMD5(
                    result.headers['content-md5']);
                if (contentMD5 && contentMD5 !== eTag) {
                    logHelper(log, 'debug', 'contentMD5 and Azure ETag do ' +
                    'not match in uploadPart', errors.BadDigest,
                    this._dataStoreName);
                    return callback(errors.InternalError);
                }
                objectInfo.eTag = eTag;
                objectInfo.numberSubParts = 1;
                return this._putSummaryPart(objectInfo, log, callback);
            });
        }
        const params = { uploadId, bucketName: this._azureContainerName,
            partNumber, size, objectKey: azureKey, contentMD5 };
        return this._putSubParts(request, params, objectInfo, log, callback);
    }


    objectPutTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        azureMD.tags = JSON.stringify(objectMD.tags);
        this._client.setBlobMetadata(this._azureContainerName, azureKey,
        azureMD, err => {
            if (err) {
                log.error('err from Azure GET data backend',
                { error: err, errorMessage: err.message, errorStack: err.stack,
                    dataStoreName: this._dataStoreName });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }

    objectDeleteTagging(key, bucket, objectMD, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const azureMD = this._getMetaHeaders(objectMD);
        this._client.setBlobMetadata(this._azureContainerName, azureKey,
        azureMD, err => {
            if (err) {
                log.error('err from Azure GET data backend',
                { error: err, errorMessage: err.message, errorStack: err.stack,
                dataStoreName: this._dataStoreName });
                return callback(errors.InternalError);
            }
            return callback();
        });
    }
}

module.exports = AzureClient;
