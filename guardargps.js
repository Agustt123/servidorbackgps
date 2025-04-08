const http = require("http");
const mysql = require("mysql2/promise");
const amqp = require("amqplib");
const { redisClient } = require("./dbconfig");

const port = 12500;
const hostname = "localhost";

process.env.TZ = 'UTC';

const dbConfig = {
  host: "localhost",
  user: "backgos",
  password: "pt25pt26pt",
  database: "gpsdata",
  waitForConnections: true,
  connectionLimit: 25,
  queueLimit: 0,
};

const pool = mysql.createPool(dbConfig);
const Atablas = {};
const dataStore = {};

const getCurrentDateString = () => {
  const currentDate = new Date();
  return `${currentDate.getFullYear()}${("0" + (currentDate.getMonth() + 1)).slice(-2)}${("0" + currentDate.getDate()).slice(-2)}`;
};

const formatLocalDate = (date) => {
  date.setHours(date.getHours() - 3);
  return date.toISOString().replace("T", " ").substring(0, 19);
};

let tableName;

async function executeWithRetry(connection, query, params, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await connection.execute(query, params);
    } catch (error) {
      if (error.code === 'ER_LOCK_DEADLOCK' && i < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, 100));
      } else {
        throw error;
      }
    }
  }
}

async function createTableIfNotExists(connection) {
  if (!Atablas[tableName]) {
    try {
      await connection.query(`SELECT 1 FROM ${tableName} LIMIT 1`);
      Atablas[tableName] = 1;
    } catch {
      const createTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        didempresa VARCHAR(50),
        cadete INT,
        superado INT DEFAULT 0,
        ilat DOUBLE,
        ilog DOUBLE,
        bateria DOUBLE,
        velocidad DOUBLE,
        idDispositivo VARCHAR(50),  
        precision_gps DOUBLE,            
        hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
autofecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,


        versionApp VARCHAR(50), 
        INDEX (didempresa),
        INDEX (cadete),
        INDEX (superado),
        INDEX (autofecha)
      )`;
      await connection.query(createTableQuery);
      Atablas[tableName] = 1;
    }
  }
}

async function insertData(connection, data) {
  const {
    empresa = "",
    ilat = "",
    ilong = "",
    cadete = "",
    bateria = "",
    velocidad = "",
    hora = null,
    precision_gps = null,
    idDispositivo = "",
    versionApp = "",
  } = data;

  const insertQuery = `INSERT INTO ${tableName} (didempresa, ilat, ilog, cadete, bateria, velocidad, superado,hora,precision_gps,idDispositivo,versionApp) VALUES (?, ?, ?, ?, ?, ?, 0,?,?,?,?)`;

  try {
    const [insertResult] = await executeWithRetry(connection, insertQuery, [empresa, ilat, ilong, cadete, bateria, velocidad, hora, precision_gps, idDispositivo, versionApp]);
    if (insertResult.affectedRows > 0) {
      const idInsertado = insertResult.insertId;
      const updateQuery = `UPDATE ${tableName} SET superado = 1 WHERE didempresa = ? AND cadete = ? AND id != ? `;
      await executeWithRetry(connection, updateQuery, [empresa, cadete, idInsertado]);
    }
  } catch (error) {
    console.error("Error al insertar datos:", error);
  }
}

async function listenToRabbitMQ() {
  let connection;
  let channel;

  const connectAndConsume = async () => {
    try {
      connection = await amqp.connect('amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672');
      channel = await connection.createChannel();
      await channel.prefetch(4000);

      const queue = 'gps';
      await channel.assertQueue(queue, { durable: true });

      channel.consume(queue, async (msg) => {
        const dataEntrada = JSON.parse(msg.content.toString());
        const dbConnection = await pool.getConnection();

        const getTableName = () => {
          const now = new Date();
          now.setHours(now.getHours() - 3);
          const year = now.getFullYear();
          const month = ("0" + (now.getMonth() + 1)).slice(-2);
          const day = ("0" + now.getDate()).slice(-2);
          return `gps_${day}_${month}_${year}`;
        };

        tableName = getTableName();

        try {
          switch (dataEntrada.operador) {
            case "guardar":
              await createTableIfNotExists(dbConnection);
              await insertData(dbConnection, dataEntrada);
              channel.ack(msg);
              break;
            case "xvariable":
              console.log("Datos en dataStore:", JSON.stringify(dataStore, null, 2));
              break;
            default:
              console.error("Operador inválido:", dataEntrada.operador);
          }
        } catch (error) {
          console.error("Error procesando mensaje:", error);
          channel.nack(msg, false, false);
        } finally {
          dbConnection.release();
        }
      }, { noAck: false });

      connection.on("error", (err) => {
        console.error("Error en la conexión:", err);
        reconnect();
      });

      connection.on("close", () => {
        console.error("Conexión cerrada. Reconectando...");
        reconnect();
      });

      channel.on("error", (err) => {
        console.error("Error en el canal:", err);
        reconnect();
      });

      channel.on("close", () => {
        console.error("Canal cerrado. Reconectando...");
        reconnect();
      });
    } catch (err) {
      console.error("Error al conectar a RabbitMQ:", err);
      setTimeout(reconnect, 5000);
    }
  };

  const reconnect = async () => {
    try {
      if (channel) await channel.close().catch(() => {});
      if (connection) await connection.close().catch(() => {});
    } catch (err) {
      console.error("Error cerrando recursos: ", err);
    }
    setTimeout(connectAndConsume, 5000);
  };

  await connectAndConsume();
}

const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('Servidor en funcionamiento\n');
});

server.listen(port, hostname, async () => {
  await listenToRabbitMQ();
});
