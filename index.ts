import Redis from 'ioredis';
import { writeFile } from 'fs/promises';

const REDIS_STREAM = 'stream';
const RANGE = 100;
const PRODUCERS_COUNT = 2;
const OUTPUT_FILE = 'result.json';

async function producer() {
    const client = new Redis();
    while (true) {
        const number = Math.floor(Math.random() * (RANGE + 1));
        await client.xadd(REDIS_STREAM, '*', 'number', number.toString(), 'timestamp', Date.now().toString());
    }
}

async function consumer() {
    const client = new Redis();
    const numbers = new Set<number>();
    const startTime = Date.now();

    while (numbers.size <= RANGE) {
        const result = await client.xread('BLOCK', 1000, 'STREAMS', REDIS_STREAM, '$');
        if (result) {
            for (const [, messages] of result) {
                for (const [, fields] of messages) {
                    const number = parseInt(fields[1], 10);
                    numbers.add(number);
                }
            }
        }
    }

    const timeSpent = Date.now() - startTime;
    const output = { timeSpent, numbersGenerated: Array.from(numbers) };
    await writeFile(OUTPUT_FILE, JSON.stringify(output, null, 2));
    console.log(`Result saved to ${OUTPUT_FILE}`);
    process.exit(0);
}

async function main() {
    for (let i = 0; i < PRODUCERS_COUNT; i++) {
        producer();
    }
    consumer();
}

main();
