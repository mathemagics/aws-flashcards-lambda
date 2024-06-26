import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, PutItemCommand, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import csv from 'csv-parser';
import { Handler } from 'aws-lambda';
import {Writable, Readable} from 'stream';
import { finished } from 'stream/promises';
import { v4 as uuidv4 } from 'uuid';

const region = process.env.TABLE_REGION;
const flashcardsTableName = process.env.FLASHCARDS_TABLE_NAME;
const certsTableName = process.env.CERTS_TABLE_NAME;

const s3Client = new S3Client({ region });
const dynamoDbClient = new DynamoDBClient({ region });

type Flashcard = {id: string; q: string, a: string};

const parseKey = (key: string) => {
    const keyParts = key.split('/');
    const partitionKey = keyParts[1];
    const fileName = keyParts[2];
    const sectionFileName = fileName.split('.')[0];
    const sectionName = sectionFileName.replace(/\s/g, "-");

    return {partitionKey, sectionName};
}

export const handler: Handler = async (event, context) => {
    const s3Event = event.Records[0].Sns.Message;
    const s3EventJson = JSON.parse(s3Event);
    const bucket = s3EventJson.Records[0].s3.bucket.name;
    const key = decodeURIComponent(s3EventJson.Records[0].s3.object.key.replace(/\+/g, ' '));

    const {partitionKey, sectionName} = parseKey(key);

    const params = {
        Bucket: bucket,
        Key: key,
    };

    const certName = partitionKey.replace(/-/g, " ")
    const flashcards: {question: string, answer:string, id:string}[] = [];

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

       (Body as Readable).pipe(csv({headers: ['question', 'answer']})).pipe(outputStream);
        await finished(outputStream);
    } catch (err) {
        const message = `Error getting object ${key} from bucket ${bucket}.`;
        throw new Error(message);
    }

    try {

        //  Create a new cert in the certs table if it doesn't exist
        //  Otherwise add a section to the sections list in the existing cert

        await dynamoDbClient.send(new UpdateItemCommand({
            TableName: certsTableName,
            Key: {
                cert_id: { S: partitionKey },
            },
            UpdateExpression: 'SET cert_name = :certName, sections = list_append(if_not_exists(sections, :empty_list), :section)',
            ExpressionAttributeValues: {
                ':section': {
                    L: [
                        { S: sectionName }
                    ]
                },
                ':empty_list': {
                    L: []
                },
                ':certName': { S: certName }
            },
        }));
    } catch (err) {
        const message = `Error putting item in DynamoDB table ${certsTableName}.`;
        throw new Error(message);
    }

    try {
        const flashcardList = flashcards.map((row) => {
            return {
                M: {
                    id: { S: uuidv4() },
                    q: { S: row.question.replace(/[\n\r\t\b\f]/g, ' ') },
                    a: { S: row.answer.replace(/[\n\r\t\b\f]/g, ' ') },
                }
            };
        });
        await dynamoDbClient.send(new PutItemCommand({
            TableName: flashcardsTableName,
            Item: {
                cert_id: { S: partitionKey },
                cert_name: { S: certName },
                section_id: { S: sectionName },
                flashcards: { L: flashcardList },
            }
        }));
    } catch (err) {
        const message = `Error putting item in DynamoDB table ${flashcardsTableName}. Error: ${err}`;
        throw new Error(message);
    }
};
