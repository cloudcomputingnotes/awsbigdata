console.log('Loading function');

var bucket = 'toptalkers-bucket-fv4ga8aac76w';
var path = 'flowlogs';
var zlib = require('zlib');
var AWS = require('aws-sdk');
AWS.config.update({region: 'us-east-1'});
var s3 = new AWS.S3();

exports.handler = function(event, context) {
    var i = 0;
    var record;
    var data = [];
    var key = path + "/" + context.invokeid + ".gz";

    //console.log(JSON.stringify(event, null, 2));
    // Serialize Operation
    function iter(err) {
        if (err) {
            context.fail(err);
            return;
        }
        record =  event.Records[i++];
        if (!record) {
            if (data.length === 0) {
                context.succeed("No data to upload");
                return;
            }
            zlib.gzip(data.join("\n"),function (err, result) {
                if (err) return iter(err);
                var params = {
                    Bucket: bucket,
                    Key: key,
                    Body: result
                };
                s3.putObject(params, function(err, data) {
                    if (err) return iter(err);
                    console.log("Successfully uploaded flowlog " + bucket + "/" + key);
                    context.succeed("Successfully processed " + event.Records.length + " record");
                    return;
                });
                return;
            });
            return;
        }
        var buffer = new Buffer(record.kinesis.data, 'base64');
        zlib.unzip(buffer, function(err, buffer) {
            if (err) return iter(err);
            var fl = JSON.parse(buffer.toString('ascii'));
            data = data.concat(fl.logEvents.map(function(item) {
                return item.message;
            })).filter(function(item) {
                return (item.indexOf('OK') == -1) ? false : true;
            });
            iter();
        });
    }
    // Start iterator
    iter();
    return;
};
