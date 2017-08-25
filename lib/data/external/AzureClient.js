const stream = require('stream');

const { errors, s3middleware } = require('arsenal');
const azure = require('azure-storage');
const createLogger = require('../multipleBackendLogger');
const logHelper = require('./utils').logHelper;
const azureMpuUtils = s3middleware.azureMpuUtils;
const objectUtils = s3middleware.objectUtils;
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

    _putSinglePart(request, params, log, cb) {
        const { bucketName, partNumber, size, objectKey, contentMD5, uploadId }
            = params;
        const totalSubParts = 1;
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
        bucketName, objectKey, passThrough, size, options,
        (err, result) => {
            if (err) {
                logHelper(log, 'error', 'err from Azure data backend ' +
                    'uploadPart', err, this._dataStoreName);
                return cb(errors.InternalError.customizeDescription(
                    `Error returned from Azure: ${err.message}`)
                );
            }
            const eTag = objectUtils.getHexMD5(result.headers['content-md5']);
            return cb(null, eTag, totalSubParts);
        });
    }

    _putSubParts(request, params, log, cb) {
        const { bucketName, partNumber, size, objectKey } = params;
        const { lastPartIndex, lastPartSize } =
            azureMpuUtils.getSubPartInfo(size);
        let lengthCounter = 0;
        let finishedStreaming = false;
        log.trace('data length is greater than max subpart size;' +
            'putting multiple parts');
        let currentStream = new stream.PassThrough();
        const hashedStream = new MD5Sum();
        request.pipe(hashedStream);

        function putNextSubPart(subPartIndex, AzureClient) {
            const blockId = azureMpuUtils.getBlockId(partNumber, subPartIndex);
            // NOTE: We try our best to calculate the sizes of each subpart
            // before streaming, but actual sizes may vary based on actual chunk
            // sizes in stream. Azure doesn't seem to use the content-length for
            // anything but returning the same content-length for the part.
            const subPartSize = subPartIndex === lastPartIndex ?
                lastPartSize : azureMpuUtils.maxSubPartSize;
            hashedStream.resume();
            return AzureClient.createBlockFromStream(blockId, bucketName,
            objectKey, currentStream, subPartSize, {}, err => {
                if (err) {
                    logHelper(log, 'error', 'err putting part to Azure',
                        err, AzureClient.dataStoreName);
                    return cb(errors.InternalError).customizeDescription(
                        `Error returned from Azure: ${err}`);
                }
                // NOTE: once this currentStream has ended, hashedStream
                // stream should be paused, having gotten to the point
                // we would stopper the data
                if (!finishedStreaming) {
                    return putNextSubPart(subPartIndex + 1, AzureClient);
                }
                const numberSubParts = subPartIndex + 1;
                log.trace('finished putting all subparts');
                hashedStream.on('hashed', () => {
                    log.trace('hashed event emitted');
                    hashedStream.removeAllListeners('hashed');
                    return cb(null, hashedStream.completedHash, numberSubParts);
                });
                // in case the hashed event was already emitted before the
                // event handler:
                if (hashedStream.completedHash) {
                    hashedStream.removeAllListeners('hashed');
                    return cb(null, hashedStream.completedHash, numberSubParts);
                }
                return undefined;
            });
        }

        // start piping data once it is registered
        hashedStream.on('end', () => {
            finishedStreaming = true;
            currentStream.end();
        });
        hashedStream.on('data', data => {
            if (lengthCounter + data.length > azureMpuUtils.maxSubPartSize) {
                // stopper the data flow
                hashedStream.pause();
                // signal end of previous data put
                currentStream.end();
                // reset lengthCounter
                lengthCounter = 0;
                currentStream = new stream.PassThrough();
            }
            currentStream.write(data);
            lengthCounter += data.length;
        });
        return putNextSubPart(0, this._client);
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

    uploadPart(request, streamingV4Params, partStream, size, key, uploadId,
    partNumber, bucket, log, callback) {
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        const contentMD5 = request.headers['content-md5'];

        const params = { bucketName: this._azureContainerName,
            partNumber, size, objectKey: azureKey, contentMD5, uploadId };
        const dataRetrievalInfo = {
            key: partNumber,
            dataStoreName: this._dataStoreName,
            dataStoreType: 'azure',
        };

        if (size === 0) {
            // Azure does not allow putting empty blocks; instead we
            // will just upload a summary part with content-length of 0.
            log.debug('0-byte part does not store data',
                { method: 'uploadPart' });
            dataRetrievalInfo.dataStoreETag = azureMpuUtils.zeroByteETag;
            dataRetrievalInfo.numberSubParts = 0;
            return callback(null, dataRetrievalInfo);
        }
        if (size <= azureMpuUtils.maxSubPartSize) {
            return this._putSinglePart(request, params, log,
            (err, dataStoreETag, numberSubParts) => {
                if (err) {
                    return callback(err);
                }
                dataRetrievalInfo.dataStoreETag = dataStoreETag;
                dataRetrievalInfo.numberSubParts = numberSubParts;
                return callback(null, dataRetrievalInfo);
            });
        }
        return this._putSubParts(request, params, log,
        (err, dataStoreETag, numberSubParts) => {
            if (err) {
                callback(err);
            }
            dataRetrievalInfo.dataStoreETag = dataStoreETag;
            dataRetrievalInfo.numberSubParts = numberSubParts;
            return callback(null, dataRetrievalInfo);
        });
    }

    abortMPU(key, uploadId, bucket, log, callback) {
        const azureBucket = this._azureContainerName;
        const azureKey = this._createAzureKey(bucket, key, this._bucketMatch);
        return this._client.deleteBlob(azureBucket, azureKey, err => {
            const dataAlreadyDeleted = true;
            if (err) {
                if (err.code === 'BlobNotFound') {
                    // when putting empty part =>
                    // we do not put any azure block to azure
                    // so no azure blob is created
                    // so when aborting => we try to delete azure blob that
                    // does not exist
                    return callback(null, dataAlreadyDeleted);
                }
                logHelper(log, 'error', 'error deleting mpu key on Azure', err,
                    this._dataStoreName);
                return callback(errors.InternalError.customizeDescription(
                    `Error returned from Azure: ${err}`));
            }
            return callback(null, dataAlreadyDeleted);
        });
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
