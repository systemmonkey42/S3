const assert = require('assert');
const async = require('async');

const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { expectedETag, uniqName, getAzureClient, getAzureContainerName }
  = require('../utils');
const { config } = require('../../../../../../lib/Config');
const azureMpuUtils = s3middleware.azureMpuUtils;
const maxSubPartSize = azureMpuUtils.maxSubPartSize;
const getBlockId = azureMpuUtils.getBlockId;

const azureLocation = 'azuretest';
const keyObject = 'putazure';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

let bucketUtil;
let s3;

function checkSubPart(key, expectedParts, cb) {
    azureClient.listBlocks(azureContainerName, key, 'all', (err, list) => {
        assert.equal(err, null, 'Expected success, got error ' +
        `on call to Azure: ${err}`);
        const uncommittedBlocks = list.UncommittedBlocks;
        const committedBlocks = list.CommittedBlocks;
        assert.strictEqual(committedBlocks, undefined);
        uncommittedBlocks.forEach((l, index) => {
            assert.strictEqual(l.Name, getBlockId(expectedParts[index].partnbr,
            expectedParts[index].subpartnbr));
        });
        cb();
    });
}

describeSkipIfNotMultiple('MultipleBackend put part to AZURE', function
describeF() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(function beforeFn() {
            this.currentTest.key = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });
        describe('with bucket location header', () => {
            beforeEach(function beforeEachFn(done) {
                async.waterfall([
                    next => s3.createBucket({ Bucket: azureContainerName,
                    }, err => next(err)),
                    next => s3.createMultipartUpload({
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, (err, res) => {
                        if (err) {
                            return next(err);
                        }
                        this.currentTest.uploadId = res.UploadId;
                        return next();
                    }),
                ], done);
            });

            afterEach(function afterEachFn(done) {
                async.waterfall([
                    next => s3.abortMultipartUpload({
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                    }, err => next(err)),
                    next => s3.deleteBucket({ Bucket: azureContainerName },
                      err => next(err)),
                ], done);
            });

            it('should put 0-byte block to Azure', function itFn(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                    PartNumber: 1,
                };
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        const eTagExpected = `"${azureMpuUtils.zeroByteETag}"`;
                        assert.strictEqual(res.ETag, eTagExpected);
                        return next(err);
                    }),
                    next => azureClient.listBlocks(azureContainerName,
                    this.test.key, 'all', err => {
                        assert.notEqual(err, null,
                            'Expected failure but got success');
                        assert.strictEqual(err.code, 'BlobNotFound');
                        next();
                    }),
                ], done);
            });

            it('should put 2 blocks to Azure', function itFn(done) {
                const body = Buffer.alloc(maxSubPartSize + 10);
                const parts = [{ partnbr: 1, subpartnbr: 0 },
                  { partnbr: 1, subpartnbr: 1 }];
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.key,
                    UploadId: this.test.uploadId,
                    PartNumber: 1,
                    Body: body,
                };
                async.waterfall([
                    next => s3.uploadPart(params, (err, res) => {
                        const eTagExpected = expectedETag(body);
                        assert.strictEqual(res.ETag, eTagExpected);
                        return next(err);
                    }),
                    next => checkSubPart(this.test.key, parts, next),
                ], done);
            });

            it('should put 5 parts bigger than maxSubPartSize to Azure',
            function it(done) {
                const body = Buffer.alloc(maxSubPartSize + 10);
                let parts = [];
                for (let i = 1; i < 6; i++) {
                    parts = parts.concat([
                      { partnbr: i, subpartnbr: 0 },
                      { partnbr: i, subpartnbr: 1 },
                    ]);
                }
                async.times(5, (n, next) => {
                    const partNumber = n + 1;
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                        PartNumber: partNumber,
                        Body: body,
                    };
                    s3.uploadPart(params, (err, res) => {
                        const eTagExpected = expectedETag(body);
                        assert.strictEqual(res.ETag, eTagExpected);
                        return next(err);
                    });
                }, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error: ${err}`);
                    checkSubPart(this.test.key, parts, done);
                });
            });

            it('should put 5 parts smaller than maxSubPartSize to Azure',
            function it(done) {
                const body = Buffer.alloc(10);
                let parts = [];
                for (let i = 1; i < 6; i++) {
                    parts = parts.concat([
                      { partnbr: i, subpartnbr: 0 },
                    ]);
                }
                async.times(5, (n, next) => {
                    const partNumber = n + 1;
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                        PartNumber: partNumber,
                        Body: body,
                    };
                    s3.uploadPart(params, (err, res) => {
                        const eTagExpected = expectedETag(body);
                        assert.strictEqual(res.ETag, eTagExpected);
                        return next(err);
                    });
                }, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error: ${err}`);
                    checkSubPart(this.test.key, parts, done);
                });
            });

            it('should put the same part twice', function itFn(done) {
                const body1 = Buffer.alloc(maxSubPartSize + 10);
                const body2 = Buffer.alloc(20);
                const parts2 = [{ partnbr: 1, subpartnbr: 0 },
                  { partnbr: 1, subpartnbr: 1 }];
                async.waterfall([
                    next => s3.uploadPart({
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                        PartNumber: 1,
                        Body: body1,
                    }, err => next(err)),
                    next => s3.uploadPart({
                        Bucket: azureContainerName,
                        Key: this.test.key,
                        UploadId: this.test.uploadId,
                        PartNumber: 1,
                        Body: body2,
                    }, (err, res) => {
                        const eTagExpected = expectedETag(body2);
                        assert.strictEqual(res.ETag, eTagExpected);
                        return next(err);
                    }),
                    next => checkSubPart(this.test.key, parts2, next),
                ], done);
            });
        });
    });
});
