import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import csv from 'csv-parser';
import { Handler } from 'aws-lambda';

const client = new S3Client({ region: 'us-east-2'});

export const handler: Handler = async (event, context) => {
    // Get the object from the event and show its content type
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const params = {
        Bucket: bucket,
        Key: key,
    };
    try {
        const command = new GetObjectCommand(params);
        const response = await client.send(command);
        const parsed = await csv(response.body);
        console.log(parsed);
    } catch (err) {
        console.log(err);
        const message = `Error getting object ${key} from bucket ${bucket}.`;
        console.log(message);
        throw new Error(message);
    }
};
