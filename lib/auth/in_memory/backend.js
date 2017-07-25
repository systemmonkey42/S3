const { auth } = require('arsenal');

const { config } = require('../../Config');
const Backend = auth.inMemory.backend.s3;
const backend = new Backend(config.authData);

config.on('authdata-update', () => {
    backend.refreshAuthData(config.authData);
});

module.exports = backend;
