const http = require("http");
const mysql = require("mysql2/promise");
const amqp = require("amqplib"); // Importar amqplib
const { redisClient } = require("./dbconfig");

const port = 12500;
const hostname = "localhost";

process.env.TZ = 'UTC';

const dbConfig = {
  host: "10.70.0.67",
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
  //    console.log(`Tabla ${tableName} ya existe.`);
    } catch {
      // Si hay un error, la tabla no existe y se debe crear
      const createTableQuery = `
        CREATE TABLE IF NOT EXISTS ${tableName} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    didempresa VARCHAR(50) NOT NULL,
    cadete INT NOT NULL,
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
)
`;
      await connection.query(createTableQuery);
      Atablas[tableName] = 1; // Agregar la tabla a Atablas
    //  console.log(`Tabla ${tableName} creada.`);
    }
  } else {
    //console.log(`Tabla ${tableName} ya está registrada en Atablas.`);
  }
}

// Función para guardar datos en Redis
// Función para guardar datos en Redis
async function saveToRedis(data) {
  const { empresa, cadete, ilat, ilong, bateria, velocidad } = data;

  const dateKey = getCurrentDateString(); // Obtener la fecha en el formato "YYYYMMDD"
  const formattedDate = `${dateKey.substring(0, 4)}_${dateKey.substring(4, 6)}_${dateKey.substring(6, 8)}`; // Formato "YYYY_MM_DD"

  const redisKey = tableName;

  // Crear la estructura en Redis
  const entry = {
    latitud: ilat,
    longitud: ilong,
    fecha: formatLocalDate(new Date()), // Formato de fecha
    bateria,
    velocidad,
  };

  try {
    // Obtener datos existentes
    const existingData = await redisClient.get(redisKey);
    let dataToStore = existingData ? JSON.parse(existingData) : {};

    if (!dataToStore[empresa]) {
      dataToStore[empresa] = {};
    }
    if (!dataToStore[empresa][cadete]) {
      dataToStore[empresa][cadete] = [];
    }

    // Agregar el nuevo registro
    dataToStore[empresa][cadete].push(entry);

    // Guardar en Redis
    await redisClient.set(redisKey, JSON.stringify(dataToStore));
  //  console.log("Datos guardados en Redis:", dataToStore);
  } catch (error) {
    console.error("Error al guardar en Redis:", error);
  }
}


// Función para insertar datos
async function insertData(connection, data) {
  const { empresa, ilat, ilong, cadete, bateria, velocidad } = data;

  // Verificación de parámetros
  if (empresa === undefined || ilat === undefined || ilong === undefined || cadete === undefined || bateria === undefined || velocidad === undefined) {
    console.error("Error: uno o más parámetros son undefined", data);
    return;
  }

  const insertQuery = `INSERT INTO ${tableName} (didempresa, ilat, ilog, cadete, bateria, velocidad, superado, autofecha) 
                        VALUES (?, ?, ?, ?, ?, ?, 0, NOW())`;

  try {
    const [insertResult] = await connection.execute(insertQuery, [empresa, ilat, ilong, cadete, bateria, velocidad]);
  //  console.log("Datos insertados:", { empresa, ilat, ilong, cadete, bateria, velocidad });

    // Verificar si se insertó un nuevo registro
    if (insertResult.affectedRows > 0) {
      const idinsertado = insertResult.insertId; // Captura el ID del nuevo registro

      const updateQuery = `
      UPDATE ${tableName}
      SET superado = 1
      WHERE didempresa = ? AND cadete = ? AND id != ?`;

      await connection.execute(updateQuery, [empresa, cadete, idinsertado]); // Usa parámetros para evitar inyección SQL
    }

    // Guardar en Redis después de insertar en la base de datos
    await saveToRedis(data);
  } catch (error) {
    console.error("Error al insertar datos:", error);
  }
}

// Función para escuchar mensajes desde RabbitMQ
async function listenToRabbitMQ() {
  const connection = await amqp.connect('amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672'); // Conectar a RabbitMQ
  const channel = await connection.createChannel();
  const queue = 'gps'; // Nombre de la cola

  await channel.assertQueue(queue, { durable: true });

  //console.log(`Esperando mensajes en la cola: ${queue}`);

  channel.consume(queue, async (msg) => {
    const dataEntrada = JSON.parse(msg.content.toString());
    //console.log("Mensaje recibido:", dataEntrada); // Log del mensaje recibido
    const dbConnection = await pool.getConnection();

    switch (dataEntrada.operador) {
      case "guardar":
        await createTableIfNotExists(dbConnection);
        await insertData(dbConnection, dataEntrada);
        updateDataStore(dataEntrada.empresa, dataEntrada.cadete, dataEntrada.ilat, dataEntrada.ilong, dataEntrada.bateria, dataEntrada.velocidad);
      //  console.log("Datos insertados desde RabbitMQ:", dataEntrada);
        break;

      case "xvariable":
     //   console.log("Datos en dataStore:", JSON.stringify(dataStore, null, 2));
        break;

      default:
        console.error("Operador inválido:", dataEntrada.operador);
    }
    dbConnection.release();
  }, { noAck: true }); 
}

// Función para simular el envío de mensajes cada 5 segundos
async function simulateMessageSending() {
  const connection = await amqp.connect('amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672'); // Conectar a RabbitMQ
  const channel = await connection.createChannel();
  const queue = 'gps'; // Nombre de la cola

  await channel.assertQueue(queue, { durable: true });

  // Enviar un mensaje cada 5 segundos
  setInterval(() => {
    console.log(Atablas,"aaaaaaa");
    
    const simulatedData = {
      operador: "guardar",
      empresa: 999, // Empresa aleatoria
      cadete: 999,
      ilat:99 , // Latitud aleatoria
      ilong:99 , // Longitud aleatoria
      bateria: 99, // Batería aleatoria
      velocidad:99  // Velocidad aleatoria
    };

    // Enviar el mensaje
    channel.sendToQueue(queue, Buffer.from(JSON.stringify(simulatedData)));
    
    console.log("Mensaje simulado enviado:", simulatedData);
  }, 5000); // Cada 5000 ms (5 segundos)
}

// Crear el servidor HTTP
const server = http.createServer((req, res) => {
  res.statusCode = 200;
  res.setHeader('Content-Type', 'text/plain');
  res.end('Servidor en funcionamiento\n');
});
//simulateMessageSending()

// Iniciar el servidor
server.listen(port, hostname, async () => {
  console.log(`Server running at http://${hostname}:${port}/`);
  await listenToRabbitMQ(); // Iniciar la escucha de RabbitMQ
});


