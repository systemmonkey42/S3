const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const constants = require('../../../../../../constants');
const { config } = require('../../../../../../lib/Config');
const { getAzureClient, getAzureContainerName, convertMD5 } =
    require('../utils');
const { createEncryptedBucketPromise } =
    require('../../../lib/utility/createEncryptedBucket');

const azureLocation = 'azuretest';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();

const bucket = 'buckettestmultiplebackendobjectcopy';
const key = `azureputkey-${Date.now()}`;
const copyKey = `azurecopyKey-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const expContentLength = '11';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const locMetaHeader = constants.objectLocationConstraintHeader.substring(11);

let bucketUtil;
let s3;
const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const copyParamBase = {
    Bucket: bucket,
    Key: copyKey,
    CopySource: `/${bucket}/${key}`,
};

function putSourceObj(location, isEmptyObj, cb) {
    const sourceParams = { Bucket: bucket, Key: key,
        Metadata: {
            'scal-location-constraint': location,
            'test-header': 'copyme',
        },
    };
    if (!isEmptyObj) {
        sourceParams.Body = body;
    }
    s3.putObject(sourceParams, (err, result) => {
        assert.equal(err, null, `Error putting source object: ${err}`);
        if (isEmptyObj) {
            assert.strictEqual(result.ETag, `"${emptyMD5}"`);
        } else {
            assert.strictEqual(result.ETag, `"${normalMD5}"`);
        }
        cb();
    });
}

function assertGetObjects(sourceKey, sourceBucket, sourceLoc, destKey,
destBucket, destLoc, azureKey, mdDirective, isEmptyObj, callback) {
    const sourceGetParams = { Bucket: sourceBucket, Key: sourceKey };
    const destGetParams = { Bucket: destBucket, Key: destKey };

    async.series([
        cb => s3.getObject(sourceGetParams, cb),
        cb => s3.getObject(destGetParams, cb),
        cb => azureClient.getBlobProperties(azureContainerName, azureKey, cb),
    ], (err, results) => {
        assert.equal(err, null, `Error in assertGetObjects: ${err}`);

        const [sourceRes, destRes, azureRes] = results;
        const convertedMD5 = convertMD5(azureRes[0].contentSettings.contentMD5);
        if (isEmptyObj) {
            assert.strictEqual(sourceRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(destRes.ETag, `"${emptyMD5}"`);
            assert.strictEqual(convertedMD5, `${emptyMD5}`);
            assert.strictEqual('0', azureRes[0].contentLength);
        } else {
            assert.strictEqual(sourceRes.ETag, `"${normalMD5}"`);
            assert.strictEqual(destRes.ETag, `"${normalMD5}"`);
            assert.deepStrictEqual(sourceRes.Body, destRes.Body);
            assert.strictEqual(convertedMD5, `${normalMD5}`);
            assert.strictEqual(expContentLength, azureRes[0].contentLength);
        }
        if (mdDirective === 'COPY') {
            assert.strictEqual(sourceRes.Metadata['test-header'],
                destRes.Metadata['test-header']);
        }
        assert.strictEqual(sourceRes.ContentLength, destRes.ContentLength);
        assert.strictEqual(sourceRes.Metadata[locMetaHeader], sourceLoc);
        assert.strictEqual(destRes.Metadata[locMetaHeader], destLoc);
        callback();
    });
}

describeSkipIfNotMultiple('MultipleBackend object copy', function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');
            if (process.env.ENABLE_KMS_ENCRYPTION === 'true') {
                s3.createBucketAsync = createEncryptedBucketPromise;
            }
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should copy an object from mem to Azure', done => {
            putSourceObj('mem', false, () => {
                const copyParams = Object.assign({
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    } }, copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        azureLocation, copyKey, 'REPLACE', false, done);
                });
            });
        });

        it('should copy an object from Azure to mem', done => {
            putSourceObj(azureLocation, false, () => {
                const copyParams = Object.assign({
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': 'mem',
                    } }, copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(key, bucket, azureLocation, copyKey,
                        bucket, 'mem', key, 'REPLACE', false, done);
                });
            });
        });

        it('should copy an object from mem to Azure and retain metadata',
        done => {
            putSourceObj('mem', false, () => {
                const copyParams = Object.assign({
                    MetadataDirective: 'COPY',
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    } }, copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        azureLocation, copyKey, 'COPY', false, done);
                });
            });
        });

        it('should copy an object on Azure', done => {
            putSourceObj(azureLocation, false, () => {
                const copyParams = Object.assign({ MetadataDirective: 'COPY' },
                    copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${normalMD5}"`);
                    assertGetObjects(key, bucket, azureLocation, copyKey,
                        bucket, azureLocation, copyKey, 'COPY', false, done);
                });
            });
        });

        it('should copy a 0-byte object from mem to Azure', done => {
            putSourceObj('mem', true, () => {
                const copyParams = Object.assign({ MetadataDirective: 'REPLACE',
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    } }, copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${emptyMD5}"`);
                    assertGetObjects(key, bucket, 'mem', copyKey, bucket,
                        azureLocation, copyKey, 'REPLACE', true, done);
                });
            });
        });

        it('should copy a 0-byte object on Azure', done => {
            putSourceObj(azureLocation, true, () => {
                const copyParams = Object.assign({ MetadataDirective: 'COPY' },
                    copyParamBase);
                s3.copyObject(copyParams, (err, result) => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error: ${err}`);
                    assert.strictEqual(result.CopyObjectResult.ETag,
                        `"${emptyMD5}"`);
                    assertGetObjects(key, bucket, azureLocation, copyKey,
                        bucket, azureLocation, copyKey, 'COPY', true, done);
                });
            });
        });

        it('should return error if Azure source object has ' +
        'been deleted', done => {
            putSourceObj(azureLocation, false, () => {
                azureClient.deleteBlob(azureContainerName, key, err => {
                    assert.equal(err, null, 'Error deleting object from ' +
                        `Azure: ${err}`);
                    const copyParams = { Bucket: bucket, Key: copyKey,
                        CopySource: `/${bucket}/${key}`,
                        MetadataDirective: 'COPY',
                    };
                    s3.copyObject(copyParams, err => {
                        assert.strictEqual(err.code, 'InternalError');
                        done();
                    });
                });
            });
        });
    });
});
