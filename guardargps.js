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

const currentDate = new Date();
currentDate.setHours(currentDate.getHours() - 3);
let year = currentDate.getFullYear();
let month = ("0" + (currentDate.getMonth() + 1)).slice(-2);
let day = ("0" + currentDate.getDate()).slice(-2);
let tableName;

async function executeWithRetry(connection, query, params, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      return await connection.execute(query, params);
    } catch (error) {
      if (error.code === 'ER_LOCK_DEADLOCK' && i < retries - 1) {
      //  console.warn(`Deadlock detectado. Reintentando ${i + 1}/${retries}...`);
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
    autofecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
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

async function saveToRedis(data) {
  const { empresa, cadete, ilat, ilong, bateria, velocidad } = data;
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
  } = data;



  const insertQuery = `INSERT INTO ${tableName} (didempresa, ilat, ilog, cadete, bateria, velocidad, superado, autofecha,hora,precision_gps,idDispositivo) VALUES (?, ?, ?, ?, ?, ?, 0, NOW(),?,?,?)`;
  try {
    const [insertResult] = await executeWithRetry(connection, insertQuery, [empresa, ilat, ilong, cadete, bateria, velocidad,hora,precision_gps,idDispositivo]);
    if (insertResult.affectedRows > 0) {
      const idInsertado = insertResult.insertId;
      const updateQuery = `UPDATE ${tableName} SET superado = 1 WHERE didempresa = ? AND cadete = ? AND id != ? `;
      await executeWithRetry(connection, updateQuery, [empresa, cadete, idInsertado]);
      
    }
    
  //  await saveToRedis(data);
    //console.log("hola");
  } catch (error) {
  //  console.error("Error al insertar datos:", error);
  }
}

async function listenToRabbitMQ() {
  let connection;
  let channel;

  const reconnect = async () => {
    try {
      if (connection) await connection.close();
      if (channel) await channel.close();

      connection = await amqp.connect('amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672');
      channel = await connection.createChannel();
      
      await channel.prefetch(4000); 

      const queue = 'gps';
      await channel.assertQueue(queue, { durable: true });

      channel.consume(queue, async (msg) => {
        const dataEntrada = JSON.parse(msg.content.toString());
        const dbConnection = await pool.getConnection();
        
        // Función para obtener el nombre de la tabla del día actual
        const getTableName = () => {
          const now = new Date();
          now.setHours(now.getHours() - 3);
          const year = now.getFullYear();
          const month = ("0" + (now.getMonth() + 1)).slice(-2);
          const day = ("0" + now.getDate()).slice(-2);
          return `gps_${day}_${month}_${year}`;
        };
      
        tableName = getTableName();  // Se actualiza en cada mensaje
      //  console.log("Procesando datos en la tabla:", tableName);
      
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
          channel.nack(msg, false, false); // Rechaza el mensaje
        } finally {
          dbConnection.release();
        }
      }, { noAck: false });
      

      // Manejo de errores en el canal
      channel.on('error', (err) => {
        console.error('Error en el canal:', err);
        setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
      });

      // Manejo de cierre del canal
      channel.on('close', () => {
        console.error('Canal cerrado. Intentando reconectar...');
        setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
      });

      // Manejo de errores en la conexión
      connection.on('error', (err) => {
        console.error('Error en la conexión:', err);
        setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
      });

      // Manejo de cierre de conexión
      connection.on('close', () => {
        console.error('Conexión cerrada. Intentando reconectar...');
        setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
      });

    } catch (err) {
      console.error('Error al conectar a RabbitMQ:', err);
      setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
    }
  };

  await reconnect();
}


const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('Servidor en funcionamiento\n');
});

server.listen(port, hostname, async () => {
  await listenToRabbitMQ();
});
