import mysql from 'mysql2/promise';
import { BufferJSON, initAuthCreds, fromObject } from '../Utils';
import {
    mysqlConfig,
    mysqlData,
    AuthenticationCreds,
    AuthenticationState,
    SignalDataTypeMap
} from '../Types';

export const useSqlAuthState = async (config: {
    host: string,
    user: string,
    password: string,
    database: string,
    tableName?: string,
    session?: string
}): Promise<{
    state: AuthenticationState;
    saveCreds: () => Promise<void>;
    clear: () => Promise<void>;
    removeCreds: () => Promise<void>;
    query: (tableName: string, docId: string) => Promise<mysqlData | null>;
}> => {
    const { host, user, password, database, tableName, session } = config;
    const connection = await mysql.createConnection({ host, user, password, database });

    const table = tableName ?? 'amiruldev_auth';
    const sessionName = session ?? `session_${Date.now()}`;

    // Function to ensure session and create creds table if not exists
    const ensureSession = async () => {
        // Create table if not exists
        await connection.execute(`
            CREATE TABLE IF NOT EXISTS \`${table}\` (
                id VARCHAR(255) PRIMARY KEY,
                value JSON,
                session VARCHAR(255)
            )
        `);

        // Check if creds entry exists
        const [credsRows]: any = await connection.execute(`SELECT * FROM \`${table}\` WHERE id = 'creds'`);
        if (credsRows.length === 0) {
            await connection.execute(`INSERT INTO \`${table}\` (id, session) VALUES ('creds', ?)`, [sessionName]);
        }
    };

    await ensureSession();

    // Function to query data from the database
    const query = async (tableName: string, docId: string): Promise<mysqlData | null> => {
        const [rows]: any = await connection.execute(`SELECT * FROM \`${tableName}\` WHERE id = ?`, [`${sessionName}-${docId}`]);
        return rows.length > 0 ? rows[0] : null;
    };

    // Function to read and parse data
    const readData = async (id: string): Promise<any> => {
        const data = await query(table, id);
        if (!data || !data.value) {
            return null;
        }
        const creds = typeof data.value === 'object' ? JSON.stringify(data.value) : data.value;
        return JSON.parse(creds, BufferJSON.reviver);
    };

    // Function to write data to the database
    const writeData = async (id: string, value: object) => {
        const valueFixed = JSON.stringify(value, BufferJSON.replacer);
        await connection.execute(
            `INSERT INTO \`${table}\` (id, value, session) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value)`,
            [`${sessionName}-${id}`, valueFixed, sessionName]
        );
    };

    // Function to remove data from the database
    const removeData = async (id: string) => {
        await connection.execute(`DELETE FROM \`${table}\` WHERE id = ?`, [`${sessionName}-${id}`]);
    };

    // Function to clear all non-creds data
    const clearAll = async () => {
        await connection.execute(`DELETE FROM \`${table}\` WHERE session = ? AND id != 'creds'`, [sessionName]);
    };

    // Function to remove all data including creds
    const removeAll = async () => {
        await connection.execute(`DELETE FROM \`${table}\` WHERE session = ?`, [sessionName]);
    };

    // Read and initialize creds, or use default if not found
    const creds: AuthenticationCreds = (await readData('creds')) || initAuthCreds();

    return {
        state: {
            creds,
            keys: {
                get: async (type, ids) => {
                    const data: {
                        [id: string]: SignalDataTypeMap[typeof type];
                    } = {};
                    for (const id of ids) {
                        let value = await readData(`${type}-${id}`);
                        if (type === 'app-state-sync-key' && value) {
                            value = fromObject(value);
                        }
                        data[id] = value;
                    }
                    return data;
                },
                set: async data => {
                    for (const category in data) {
                        for (const id in data[category]) {
                            const value = data[category][id];
                            const name = `${category}-${id}`;
                            if (value) {
                                await writeData(name, value);
                            } else {
                                await removeData(name);
                            }
                        }
                    }
                }
            }
        },
        saveCreds: async () => {
            await writeData('creds', creds); // Save creds to the database
        },
        clear: async () => {
            await clearAll(); // Clear non-creds data
        },
        removeCreds: async () => {
            await removeAll(); // Remove all data including creds
        },
        query: async (tableName: string, docId: string) => {
            return await query(tableName, docId); // Query data from the database
        }
    };
};
