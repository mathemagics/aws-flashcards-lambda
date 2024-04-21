import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import csv from 'csv-parser';
import { Handler } from 'aws-lambda';
import {Writable, Readable} from 'stream';
import { finished } from 'stream/promises';

const tableName = process.env.TABLE_NAME;

const s3Client = new S3Client({ region: 'us-east-2'});
const dynamoDbClient = new DynamoDBClient({ region: 'us-east-2'});

type Flashcard = {key: string, value: string};

export const handler: Handler = async (event, context) => {
    // Get the object from the event and show its content type
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const params = {
        Bucket: bucket,
        Key: key,
    };
    const keyParts = key.split('/');
    const partitionKey = keyParts[1];
    const fileName = keyParts[2];
    const sectionName = fileName.split('.')[0];

    const flashcards: Flashcard[] = [];

    try {
        const command = new GetObjectCommand(params);
        const {Body} = await s3Client.send(command);
        if (!Body) throw new Error("Failed to get body from S3 object");

        const outputStream = new Writable({
            objectMode: true,
            write(row, encoding: BufferEncoding, callback: (error?: Error | null) => void) {
                flashcards.push(row);
                callback();
            }
        });

       (Body as Readable).pipe(csv()).pipe(outputStream);
        await finished(outputStream);
    } catch (err) {
        console.log(err);
        const message = `Error getting object ${key} from bucket ${bucket}.`;
        console.log(message);
        throw new Error(message);
    }

    try {
        const res = await dynamoDbClient.send(new PutItemCommand({
            TableName: tableName,
            Item: {
                cert_id: { S: partitionKey },
                section_id: { S: sectionName },
                flashcards: { S: JSON.stringify(flashcards) },
            }
        }));
        return res
    } catch (err) {
        console.log(err);
        const message = `Error putting item in DynamoDB table ${tableName}.`;
        console.log(message);
        throw new Error(message);
    }
};
