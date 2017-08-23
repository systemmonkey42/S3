'use strict'; // eslint-disable-line strict
const assert = require('assert');
const DummyRequestLogger = require('../unit/helpers').DummyRequestLogger;
const clientCheck
    = require('../../lib/utilities/healthcheckHandler').clientCheck;

const log = new DummyRequestLogger();

describe('Healthcheck response', () => {
    it('should return no error', done => {
        clientCheck(log, err => {
            assert.strictEqual(err, null,
                `Expected success but got error ${err}`);
            done();
        });
    });
});
