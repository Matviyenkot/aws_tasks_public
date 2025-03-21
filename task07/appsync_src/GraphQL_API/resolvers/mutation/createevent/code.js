import { v4 as uuidv4 } from 'uuid';
import { DynamoDBClient, PutItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb';
import { unmarshall, marshall } from '@aws-sdk/util-dynamodb';

const dynamoDB = new DynamoDBClient({ region: process.env.region});
const EVENTS_TABLE = process.env.event_table;

export const handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2));

    try {
        switch (event.info.fieldName) {
            case 'createEvent':
                return await createEvent(event.arguments);
            case 'getEventById':
                return await getEventById(event.arguments.id);
            default:
                throw new Error(`Unknown field: ${event.info.fieldName}`);
        }
    } catch (error) {
        console.error("Error handling request:", error);
        throw new Error(error.message);
    }
};

async function createEvent({ userId, payLoad }) {
    const eventId = uuidv4();
    const createdAt = new Date().toISOString();
    const item = {
        id: { S: eventId },
        userId: { N: userId.toString() },
        createdAt: { S: createdAt },
        payLoad: { S: payLoad }
    };

    await dynamoDB.send(new PutItemCommand({ TableName: EVENTS_TABLE, Item: item }));

    return { id: eventId, createdAt };
}

async function getEventById(id) {
    const response = await dynamoDB.send(new GetItemCommand({
        TableName: EVENTS_TABLE,
        Key: { id: { S: id } }
    }));

    if (!response.Item) {
        throw new Error("Event not found");
    }

    return unmarshall(response.Item);
}
