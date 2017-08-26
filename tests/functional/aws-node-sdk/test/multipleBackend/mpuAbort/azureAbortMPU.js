const assert = require('assert');
const async = require('async');

const { s3middleware } = require('arsenal');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { uniqName, getAzureClient, getAzureContainerName }
    = require('../utils');
const { config } = require('../../../../../../lib/Config');
const azureMpuUtils = s3middleware.azureMpuUtils;
const maxSubPartSize = azureMpuUtils.maxSubPartSize;

const azureLocation = 'azuretest';
const keyObject = 'abortazure';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName();

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

let bucketUtil;
let s3;

describeSkipIfNotMultiple('Abort MPU on Azure data backend', function
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
                    next => s3.createBucket({ Bucket: azureContainerName },
                        err => next(err)),
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

            afterEach(done => s3.deleteBucket({ Bucket: azureContainerName },
                done));

            describe('with one empty part', () => {
                beforeEach(function beFn(done) {
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                        PartNumber: 1,
                    };
                    s3.uploadPart(params, err => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error: ${err}`);
                        return done();
                    });
                });
                it('should abort empty part ', function itFn(done) {
                    async.waterfall([
                        next => s3.abortMultipartUpload({
                            Bucket: azureContainerName,
                            Key: this.test.key,
                            UploadId: this.test.uploadId,
                        }, err => next(err)),
                        next => azureClient.getBlobProperties(
                        azureContainerName,
                        this.test.key, err => {
                            assert.strictEqual(err.statusCode, 404);
                            assert.strictEqual(err.code, 'NotFound');
                            return next();
                        }),
                    ], done);
                });
            });

            describe('with one part bigger that max subpart', () => {
                beforeEach(function beFn(done) {
                    const body = Buffer.alloc(maxSubPartSize + 10);
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        UploadId: this.currentTest.uploadId,
                        PartNumber: 1,
                        Body: body,
                    };
                    s3.uploadPart(params, err => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error: ${err}`);
                        return done();
                    });
                });
                it('should delete the key on Azure when aborting MPU ',
                function itFn(done) {
                    async.waterfall([
                        next => s3.abortMultipartUpload({
                            Bucket: azureContainerName,
                            Key: this.test.key,
                            UploadId: this.test.uploadId,
                        }, err => next(err)),
                        next => azureClient.getBlobProperties(
                        azureContainerName,
                        this.test.key, err => {
                            assert.strictEqual(err.statusCode, 404);
                            assert.strictEqual(err.code, 'NotFound');
                            return next();
                        }),
                    ], done);
                });
            });
        });
    });
});
