const http = require("http");
const mysql = require("mysql2/promise");
const amqp = require("amqplib");

const port = 12500;
const hostname = "localhost";
const Adbcreada = {};

process.env.TZ = "UTC";

const dbConfig = {
  // host: "149.56.182.49",
  // port: 44335,
  // user: "root",
  // password: "pJ3AjsA3d8Qw3A",
  host: "localhost",
  user: "backgos",
  password: "pt25pt26pt",
  database: "gpsdata",
  waitForConnections: true,
  connectionLimit: 25,
  queueLimit: 0,
};

const pool = mysql.createPool(dbConfig);
let tableName;

// Formatea la fecha local para el nombre de la tabla
const getTableName = () => {
  const now = new Date();
  now.setHours(now.getHours() - 3);
  const year = now.getFullYear();
  const month = ("0" + (now.getMonth() + 1)).slice(-2);
  const day = ("0" + now.getDate()).slice(-2);
  return `gps_${day}_${month}_${year}`;
};

// Reintentos en caso de deadlock
async function executeWithRetry(connection, query, params, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await connection.execute(query, params);
    } catch (error) {
      if (error.code === "ER_LOCK_DEADLOCK" && i < retries - 1) {
        await new Promise((r) => setTimeout(r, 100));
      } else {
        throw error;
      }
    }
  }
}

// Crea la tabla si no existe
async function createTableIfNotExists(connection, fechaStr) {
  if (Adbcreada[fechaStr]) return;
  const tbl = fechaStr.replace(/-/g, "_");
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS \`${tbl}\` (
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
      autofecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      versionApp VARCHAR(50),
      INDEX (didempresa),
      INDEX (cadete),
      INDEX (superado),
      INDEX (autofecha)
    )`;
  await connection.query(createTableQuery);
  Adbcreada[fechaStr] = true;
}

// Inserta datos y marca anteriores como superado
async function insertData(connection, data) {
  const {
    empresa = "",
    ilat = "",
    ilog = "",
    cadete = "",
    bateria = "",
    velocidad = "",
    hora = null,
    precision = null,
    idDispositivo = "",
    versionApp = "",
  } = data;

  const insertQuery = `INSERT INTO ${tableName}
    (didempresa, ilat, ilog, cadete, bateria, velocidad, superado, hora, precision_gps, idDispositivo, versionApp)
    VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)`;

  const [result] = await executeWithRetry(connection, insertQuery, [
    empresa, ilat, ilog, cadete, bateria, velocidad, hora, precision, idDispositivo, versionApp
  ]);

  if (result.affectedRows > 0) {
    const idInsertado = result.insertId;
    const updateQuery = `
      UPDATE ${tableName}
      SET superado = 1
      WHERE didempresa = ? AND cadete = ? AND id != ?`;
    await executeWithRetry(connection, updateQuery, [empresa, cadete, idInsertado]);
  }
}

// --------------------------------------------------
//             RABBITMQ: escucha y reconexión
// --------------------------------------------------

let isReconnecting = false;

async function listenToRabbitMQ() {
  let connection;
  let channel;
  let reconnectTimer = null;

  const setupErrorHandlers = (conn, ch) => {
    conn.once("error", (err) => {
      console.error("Error en la conexión:", err);
      scheduleReconnect();
    });
    conn.once("close", () => {
      console.error("Conexión cerrada. Reconectando...");
      scheduleReconnect();
    });
    ch.once("error", (err) => {
      console.error("Error en el canal:", err);
      scheduleReconnect();
    });
    ch.once("close", () => {
      console.error("Canal cerrado. Reconectando...");
      scheduleReconnect();
    });
  };

  const connectAndConsume = async () => {
    try {
      connection = await amqp.connect("amqp://guest:guest@192.168.1.97:5672");
      channel = await connection.createChannel();
      await channel.prefetch(100); // más razonable que 4000
      await channel.assertQueue("gps", { durable: true });

      setupErrorHandlers(connection, channel);

      console.log("Esperando mensajes en la cola 'gps'...");
      channel.consume("gps", async (msg) => {
        if (!msg) return;
        const content = msg.content.toString();
        console.log("Mensaje recibido:", content);

        const dataEntrada = JSON.parse(content);
        const dbConnection = await pool.getConnection();
        tableName = getTableName();

        try {
          switch (dataEntrada.operador) {
            case "guardar":
              await createTableIfNotExists(dbConnection, tableName);
              await insertData(dbConnection, dataEntrada);
              channel.ack(msg);
              break;
            case "xvariable":
              console.log("dataStore:", JSON.stringify(dataStore, null, 2));
              channel.ack(msg);
              break;
            default:
              channel.nack(msg, false, false);
          }
        } catch (err) {
          console.error("Error procesando mensaje:", err);
          channel.nack(msg, false, false);
        } finally {
          dbConnection.release();
        }
      }, { noAck: false });

      // Si llegamos aquí, la conexión fue exitosa
      isReconnecting = false;
    } catch (err) {
      console.error("Error al conectar a RabbitMQ:", err);
      scheduleReconnect();
    }
  };

  const scheduleReconnect = () => {
    if (isReconnecting) return;
    isReconnecting = true;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(async () => {
      try {
        if (channel) await channel.close().catch(() => { });
        if (connection) await connection.close().catch(() => { });
      } catch (_) { }
      await connectAndConsume();
    }, 5000);
  };

  // Arrancamos la primera conexión
  await connectAndConsume();
}

// --------------------------------------------------
//             SERVIDOR HTTP
// --------------------------------------------------

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Servidor en funcionamiento\n");
});

server.listen(port, hostname, async () => {
  console.log(`HTTP server escuchando en http://${hostname}:${port}/`);
  await listenToRabbitMQ();
});
