const express = require("express");
const mysql = require("mysql2/promise");
const bodyParser = require("body-parser");

const app = express();
const port = process.env.PORT || 13000; // Usa un puerto definido en la variable de entorno o 13000 por defecto
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

// Middleware para parsear el cuerpo de las solicitudes
app.use(bodyParser.json());

const getCurrentDateString = () => {
  const currentDate = new Date();
  return `${currentDate.getFullYear()}${("0" + (currentDate.getMonth() + 1)).slice(-2)}${("0" + currentDate.getDate()).slice(-2)}`;
};

const formatLocalDate = (date) => {
  date.setHours(date.getHours() - 3);
  return date.toISOString().replace("T", " ").substring(0, 19);
};

const currentDate = new Date();
const year = currentDate.getFullYear();
const month = ("0" + (currentDate.getMonth() + 1)).slice(-2);
const day = ("0" + currentDate.getDate()).slice(-2);

const tableName = `gps_${day}_${month}_${year}`;

// Definición de rutas
app.post("/consultar", async (req, res) => {
  const dataEntrada = req.body;
  const connection = await pool.getConnection();
  
  try {
    switch (dataEntrada.operador) {
      case "getActual":
        await getActualData(connection, dataEntrada, res);
        break;
      case "getHistorial":
        await getHistorial(connection, dataEntrada, res);
        break;
      case "getAll":
        await getAll(connection, dataEntrada, res);
        break;
      case "cadeteFiltrado":
        await obtenerHorasCadetesPorFecha(connection, dataEntrada, res);
        break;
      case "cadeteFiltradoUnico":
        await obtenerHorasCadetePorFecha(connection, dataEntrada, res);
        break;
      case "xvariable":
        res.status(400).json(dataStore);
        break;
      default:
        res.status(400).json({ error: "Operador inválido" });
    }
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error en el servidor" });
  } finally {
    connection.release();
  }
});


async function getActualData(connection, data, res) {
  const query = `SELECT ilat, ilog, bateria, velocidad, DATE_FORMAT(autofecha, '%d/%m/%Y %H:%i') as autofecha 
                 FROM ${tableName} WHERE didempresa = ? AND cadete = ? AND superado = 0 
                 ORDER BY autofecha DESC LIMIT 1`;
  const [results] = await connection.execute(query, [data.empresa, data.cadete]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results[0] || { ilat: 0, ilog: 0, bateria: 0, velocidad: 0 }));
}

// Función para obtener historial
async function getHistorial(connection, data, res) {
  const query = `SELECT * FROM ${tableName} WHERE didempresa = ? AND cadete = ?`;
  const [results] = await connection.execute(query, [data.empresa, data.cadete]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results));
}

// Función para obtener todos los datos
async function getAll(connection, data, res) {
  const query = `SELECT * FROM ${tableName} WHERE superado = 0 AND didempresa = ?`;
  const [results] = await connection.execute(query, [data.didempresa]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results));
}

// Función para obtener horas de cadetes por fecha
async function obtenerHorasCadetesPorFecha(connection, data, res) {
  const query = `SELECT * FROM ${tableName} WHERE didempresa = ? AND DATE(autofecha) = ?`;
  const [results] = await connection.execute(query, [data.didempresa, data.fecha]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results));
}

// Función para obtener horas de un cadete por fecha
async function obtenerHorasCadetePorFecha(connection, data, res) {
  const query = `SELECT * FROM ${tableName} WHERE didempresa = ? AND cadete = ? AND DATE(autofecha) = ?`;
  const [results] = await connection.execute(query, [data.didempresa, data.cadete, data.fecha]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results));
}

// Simulación de datos
const simulateDataInsertion = () => {
  setInterval(async () => {
    const simulatedData = {
      operador: "guardar",
      empresa: 4,
      cadete: 1,
      ilat: 1,
      ilong: 6,
      bateria: 11,
      velocidad: 12,
    };

    const options = {
      hostname: hostname,
      port: port,
      path: '/',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(JSON.stringify(simulatedData))
      }
    };

    const req = http.request(options, (res) => {
      res.on('data', (d) => {
        process.stdout.write(d);
      });
    });

    req.on('error', (error) => {
      console.error(error);
    });

    req.write(JSON.stringify(simulatedData));
    req.end();
  }, 5000); // Cada 5 segundos
};

// Iniciar la simulación
//simulateDataInsertion();

// Iniciar el servidor
app.listen(port, hostname, () => {
  console.log(`Server running at http://${hostname}:${port}/`);
});
