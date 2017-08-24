const assert = require('assert');

const { config } = require('../../../../../../lib/Config');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

const azureLocation = 'azuretest';
let azureContainerName;

if (config.locationConstraints[azureLocation] &&
config.locationConstraints[azureLocation].details &&
config.locationConstraints[azureLocation].details.azureContainerName) {
    azureContainerName =
        config.locationConstraints[azureLocation].details.azureContainerName;
}

const keyName = `somekey-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(104860000);

let s3;
let bucketUtil;

describeSkipIfNotMultiple.only('Azure data backend', () => {
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        describe('MPU put part', () => {
            beforeEach(
                function beforeFn(done) {
                    s3.createBucket({ Bucket: azureContainerName,
                        CreateBucketConfiguration: {
                            LocationConstraint: azureLocation,
                        },
                    }, err => {
                        assert.equal(err, null, `Err creating bucket ${err}`);
                        const params = {
                            Bucket: azureContainerName,
                            Key: keyName,
                            Metadata:
                                { 'scal-location-constraint': azureLocation },
                        };
                        s3.createMultipartUpload(params, (err, res) => {
                            assert.equal(err, null, `Err in init mpu: ${err}`);
                            this.currentTest.uploadId = res.UploadId;
                            done();
                        });
                    });
                }
            );
            afterEach(function afterFn(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: keyName,
                    UploadId: this.currentTest.uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });

            it('should put a part to Azure',
            function itFn(done) {
                const params = { Bucket: azureContainerName, Key: keyName,
                    PartNumber: 1, UploadId: this.test.uploadId, Body: body };
                s3.uploadPart(params, (err, result) => {
                    console.log('\n\n-------------RESULTS1------------:\n', result);
                    assert.equal(err, null);
                    done();
                });   
            });

            it('should put a 0-byte part to Azure',
            function itFn(done) {
                const params = { Bucket: azureContainerName, Key: keyName,
                    PartNumber: 1, UploadId: this.test.uploadId };
                s3.uploadPart(params, (err, result) => {
                    console.log('\n\n-------------RESULTS222------------:\n', result);
                    assert.equal(err, null);
                    done();
                });
            });

            it('should put a part larger than Azure max part size',
            function itFn(done) {
                const params = { Bucket: azureContainerName, Key: keyName,
                    PartNumber: 1, UploadId: this.test.uploadId,
                    Body: bigBody };
                s3.uploadPart(params, (err, result) => {
                    console.log('\n\n-------------RESULTS33333------------:\n', result);
                    assert.equal(err, null);
                    done();
                });
            });
        });
    });
});
