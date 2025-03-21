import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
import { v4 as uuidv4 } from "uuid";

const client = new DynamoDBClient({ region: process.env.region });
const docClient = DynamoDBDocumentClient.from(client);
const TABLE_NAME = process.env.event_table;

export const handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2));

    switch (event.info.fieldName) {
        case "createEvent":
            return await createEvent(event.arguments);
        case "getEventById":
            return await getEventById(event.arguments.id);
        default:
            throw new Error("Unknown field, unable to resolve " + event.info.fieldName);
    }
};

const createEvent = async ({ userId, payLoad }) => {
    const id = uuidv4();
    const createdAt = new Date().toISOString();
    const item = { id, userId, createdAt, payLoad: JSON.parse(payLoad) };

    await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
    return { id, createdAt };
};

const getEventById = async (id) => {
    const { Item } = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { id } }));
    return Item || null;
};
