

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
currentDate.setHours(currentDate.getHours() - 3); // Restar 3 horas

const year = currentDate.getFullYear();
const month = ("0" + (currentDate.getMonth() + 1)).slice(-2);
const day = ("0" + currentDate.getDate()).slice(-2);

const tableName = `gps_${day}_${month}_${year}`;
const dataStore = {};

// Función para actualizar el almacén de datos
const updateDataStore = (empresa, chofer, latitud, longitud, bateria, velocidad) => {
  const dateKey = getCurrentDateString();
  if (!dataStore[dateKey]) {
    dataStore[dateKey] = {};
  }
  if (!dataStore[dateKey][empresa]) {
    dataStore[dateKey][empresa] = {};
  }
  if (!dataStore[dateKey][empresa][chofer]) {
    dataStore[dateKey][empresa][chofer] = [];
  }
  dataStore[dateKey][empresa][chofer].push({ 
    latitud, 
    longitud, 
    fecha: formatLocalDate(new Date()), 
    bateria, 
    velocidad 
  });
};

// Función para crear la tabla si no existe
async function createTableIfNotExists(connection) {
  if (!Atablas[tableName]) {
    try {
      // Verificar si la tabla existe en la base de datos
      await connection.query(`SELECT 1 FROM ${tableName} LIMIT 1`);
      // Si la consulta se ejecuta sin errores, la tabla existe
      Atablas[tableName] = 1; // Agregar la tabla a Atablas
    } catch {
      // Si hay un error, la tabla no existe y se debe crear
      const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
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
      Atablas[tableName] = 1; // Agregar la tabla a Atablas
    }
  }
}

// Función para guardar datos en Redis
async function saveToRedis(data) {
  const { empresa, cadete, ilat, ilong, bateria, velocidad } = data;

  const dateKey = getCurrentDateString();
  const formattedDate = `${dateKey.substring(0, 4)}_${dateKey.substring(4, 6)}_${dateKey.substring(6, 8)}`;

  const redisKey = tableName;

  const entry = {
    latitud: ilat,
    longitud: ilong,
    fecha: formatLocalDate(new Date()),
    bateria,
    velocidad,
  };

  try {
    const existingData = await redisClient.get(redisKey);
    let dataToStore = existingData ? JSON.parse(existingData) : {};

    if (!dataToStore[empresa]) {
      dataToStore[empresa] = {};
    }
    if (!dataToStore[empresa][cadete]) {
      dataToStore[empresa][cadete] = [];
    }

    dataToStore[empresa][cadete].push(entry);

    await redisClient.set(redisKey, JSON.stringify(dataToStore));
  } catch (error) {
    console.error("Error al guardar en Redis:", error);
  }
}

// Función para insertar datos de forma segura (usando transacciones)
async function insertData(connection, data) {
  const { empresa, ilat, ilong, cadete, bateria, velocidad } = data;

  if (empresa === undefined || ilat === undefined || ilong === undefined || cadete === undefined || bateria === undefined || velocidad === undefined) {
    console.error("Error: uno o más parámetros son undefined", data);
    return;
  }

  const insertQuery = `INSERT INTO ${tableName} (didempresa, ilat, ilog, cadete, bateria, velocidad, superado, autofecha) 
                        VALUES (?, ?, ?, ?, ?, ?, 0, NOW())`;

  const updateQuery = `UPDATE ${tableName} SET superado = 1 WHERE didempresa = ? AND cadete = ? AND id != ? LIMIT 100`;

  let attempt = 0;
  const maxRetries = 3;

  while (attempt < maxRetries) {
    const dbConnection = await pool.getConnection();
    await dbConnection.beginTransaction();

    try {
      // Insertar datos
      const [insertResult] = await dbConnection.execute(insertQuery, [empresa, ilat, ilong, cadete, bateria, velocidad]);

      if (insertResult.affectedRows > 0) {
        const idinsertado = insertResult.insertId;
        // Actualizar registros
        await dbConnection.execute(updateQuery, [empresa, cadete, idinsertado]);
      }

      // Confirmar transacción
      await dbConnection.commit();

      // Guardar en Redis después de insertar en la base de datos
      await saveToRedis(data);

      dbConnection.release();
      break; // Transacción exitosa, salir del bucle

    } catch (error) {
      await dbConnection.rollback();
      if (error.code === 'ER_LOCK_DEADLOCK') {
        attempt++;
        console.error(`Deadlock detectado. Intentando de nuevo (${attempt}/${maxRetries})`);
        if (attempt === maxRetries) {
          console.error('Maximo número de intentos alcanzado, no se pudo completar la transacción.');
        }
      } else {
        console.error("Error al insertar datos:", error);
        dbConnection.release();
        break;
      }
    }
  }
}

// Función para escuchar mensajes desde RabbitMQ
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
      default:
        console.error("Operador inválido:", dataEntrada.operador);
    }

    dbConnection.release();
  }, { noAck: true });
}

// Crear el servidor HTTP
const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('Servidor en funcionamiento\n');
});

// Iniciar el servidor
server.listen(port, hostname, async () => {
  await listenToRabbitMQ(); // Iniciar la escucha de RabbitMQ
});

