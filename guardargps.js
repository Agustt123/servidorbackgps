const http = require("http");
const mysql = require("mysql2/promise");
const amqp = require("amqplib"); // Importar amqplib
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
  connectionLimit: 10,
  queueLimit: 0,
};

const pool = mysql.createPool(dbConfig);

const Atablas = {};

const getCurrentDateString = () => {
  const currentDate = new Date();
  return `${currentDate.getFullYear()}${("0" + (currentDate.getMonth() + 1)).slice(-2)}${("0" + currentDate.getDate()).slice(-2)}`;
};

const formatLocalDate = (date) => {
  date.setHours(date.getHours() - 3);
  return date.toISOString().replace("T", " ").substring(0, 19);
};

const currentDate = new Date();
currentDate.setHours(currentDate.getHours() - 3);

const year = currentDate.getFullYear();
const month = ("0" + (currentDate.getMonth() + 1)).slice(-2);
const day = ("0" + currentDate.getDate()).slice(-2);

const tableName = `gps_${day}_${month}_${year}`;
const dataStore = {};

const updateDataStore = (empresa, chofer, latitud, longitud, bateria, velocidad) => {
  const dateKey = getCurrentDateString();
  if (!dataStore[dateKey]) dataStore[dateKey] = {};
  if (!dataStore[dateKey][empresa]) dataStore[dateKey][empresa] = {};
  if (!dataStore[dateKey][empresa][chofer]) dataStore[dateKey][empresa][chofer] = [];
  dataStore[dateKey][empresa][chofer].push({ latitud, longitud, fecha: formatLocalDate(new Date()), bateria, velocidad });
};

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
        autofecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
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

async function saveToRedis(data) {
  const { empresa, cadete, ilat, ilong, bateria, velocidad } = data;
  const dateKey = getCurrentDateString();
  const redisKey = tableName;
  const entry = { latitud: ilat, longitud: ilong, fecha: formatLocalDate(new Date()), bateria, velocidad };

  try {
    const existingData = await redisClient.get(redisKey);
    let dataToStore = existingData ? JSON.parse(existingData) : {};
    if (!dataToStore[empresa]) dataToStore[empresa] = {};
    if (!dataToStore[empresa][cadete]) dataToStore[empresa][cadete] = [];
    dataToStore[empresa][cadete].push(entry);
    await redisClient.set(redisKey, JSON.stringify(dataToStore));
  } catch (error) {
    console.error("Error al guardar en Redis:", error);
  }
}

async function insertData(data) {
  const { empresa, ilat, ilong, cadete, bateria, velocidad } = data;
  if (!empresa || !ilat || !ilong || !cadete || !bateria || !velocidad) {
    console.error("Error: uno o más parámetros son undefined", data);
    return;
  }

  let connection;
  try {
    connection = await pool.getConnection(); // Obtener conexión del pool
    await connection.beginTransaction(); // Iniciar transacción

    // Insertar datos
    const insertQuery = `INSERT INTO ${tableName} 
      (didempresa, ilat, ilog, cadete, bateria, velocidad, superado, autofecha) 
      VALUES (?, ?, ?, ?, ?, ?, 0, NOW())`;
    
    const [insertResult] = await connection.execute(insertQuery, [empresa, ilat, ilong, cadete, bateria, velocidad]);
    const idInsertado = insertResult.insertId;

    // Actualizar registros antiguos
    const updateQuery = `UPDATE ${tableName} 
      SET superado = 1 
      WHERE didempresa = ? AND cadete = ? AND id != ? 
      LIMIT 100`;
    
    await connection.execute(updateQuery, [empresa, cadete, idInsertado]);

    await connection.commit(); // Confirmar transacción
    await saveToRedis(data); // Guardar en Redis después de la base de datos
  } catch (error) {
    if (connection) await connection.rollback(); // Revertir si hay error
    console.error("Error al insertar datos:", error);
  } finally {
    if (connection) connection.release(); // Cerrar conexión
  }
}


async function listenToRabbitMQ() {
  const connection = await amqp.connect('amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672');
  const channel = await connection.createChannel();
  const queue = 'gps';
  await channel.assertQueue(queue, { durable: true });
  channel.consume(queue, async (msg) => {
    const dataEntrada = JSON.parse(msg.content.toString());
    const dbConnection = await pool.getConnection();
    switch (dataEntrada.operador) {
      case "guardar":
        await createTableIfNotExists(dbConnection);
        await insertData(dbConnection, dataEntrada);
        updateDataStore(dataEntrada.empresa, dataEntrada.cadete, dataEntrada.ilat, dataEntrada.ilong, dataEntrada.bateria, dataEntrada.velocidad);
        break;
      case "xvariable":
        console.log("Datos en dataStore:", JSON.stringify(dataStore, null, 2));
        break;
      default:
        console.error("Operador inválido:", dataEntrada.operador);
    }
    dbConnection.release();
  }, { noAck: true });
}

const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('Servidor en funcionamiento\n');
});

server.listen(port, hostname, async () => {
  await listenToRabbitMQ();
});

