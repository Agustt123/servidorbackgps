//require('dotenv').config();
const express = require('express');
const mysql = require("mysql2/promise");
const redis = require('redis');

const app = express();
const port = process.env.PORT || 13000;
let redisClient;

app.use(express.json()); // Middleware para parsear JSON

// Configuración de la base de datos
const pool = mysql.createPool({
  host: '10.70.0.67',
  user: 'backgos',
  password: 'pt25pt26pt',
  database: 'gpsdata',
 
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Verificar conexión a la base de datos
(async () => {
  try {
      const connection = await pool.getConnection();
     // console.log('Conectado a la base de datos MySQL');
      connection.release();

      // Configuración de Redis
      const redisClient = redis.createClient({
          socket: {
              host: '192.99.190.137', // IP interna
              port: 50301,            // Puerto interno
          },
          password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg', // Contraseña para autenticación
      });

      // Manejo de errores de Redis
      redisClient.on('error', (err) => {
          console.error('Error al conectar con Redis:', err);
      });

      redisClient.on('connect', () => {
         // console.log('Conectado a Redis correctamente');
      });

  } catch (error) {
      console.error('Error conectando a la base de datos:', error);
      process.exit(1); // Salir de la aplicación si no se puede conectar
  }
})();

async function getActualData(connection, data, res, tableName){
	const query = `SELECT ilat, ilog, bateria, velocidad, DATE_FORMAT(autofecha, '%d/%m/%Y %H:%i') as autofecha 
                 FROM ${tableName} WHERE didempresa = ? AND cadete = ? AND superado = 0 
                 ORDER BY autofecha DESC LIMIT 1`;

                 
                 
  const [results] = await connection.execute(query, [data.empresa, data.cadete]);

  res.writeHead(200, { "Content-Type": "application/json" });
 
  
  res.end(JSON.stringify(results[0] || { ilat: 0, ilog: 0, bateria: 0, velocidad: 0 }));
}

async function getHistorial(connection, data, res , tableName) {
  const query = `SELECT * FROM ${tableName} WHERE didempresa = ? AND cadete = ?`;
  const [results] = await connection.execute(query, [data.empresa, data.cadete]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results));
}

async function getAll(connection, data, res, tableName) {
  const query = `SELECT * FROM ${tableName} WHERE superado = 0 AND didempresa = ?`;
  const [results] = await connection.execute(query, [data.didempresa]);
  const response = {
    gps: results
  };

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(response));
}

async function obtenerHorasCadetesPorFecha(connection, data, res) {
//  console.log("aaa");
  
  // Obtener la fecha en formato YYYY-MM-DD
  const fecha = data.fecha; // Por ejemplo, "2025-02-05"
  const [year, month, day] = fecha.split('-');
  
  // Generar el nombre de la tabla sin espacios
  const claveFechadb = `gps_${day}_${month}_${year}`; // Esto debe ser gps_05_02_2025

  // Verificar el nombre de la tabla
  //console.log(`Nombre de la tabla: '${claveFechadb}'`); // Asegúrate de que no haya espacios

  // Modificar la consulta para extraer solo la parte de la fecha de autofecha
  const query = `SELECT * FROM ${claveFechadb} WHERE didempresa = ? AND autofecha LIKE ?`;

  //console.log(query); // Asegúrate de que se imprima correctamente el nombre de la tabla
  const [results] = await connection.execute(query, [data.didempresa, `${data.fecha}%`]);

  // Estructurar la respuesta
  const response = {};
  
  // Agrupar resultados por empresa
  results.forEach(row => {
    const empresaId = row.didempresa; // Suponiendo que este es el ID de la empresa
    const choferId = row.cadete; // Suponiendo que este es el ID del chofer

    // Inicializar la estructura de la empresa si no existe
    if (!response[empresaId]) {
      response[empresaId] = {};
    }

    if (!response[empresaId][choferId]) {
      response[empresaId][choferId] = { coordenadas: [] }; // Inicializar coordenadas como un array
    }

    // Formatear autofecha
    const formattedAutofecha = formatFecha(row.autofecha);
  

    // Agregar las coordenadas
    response[empresaId][choferId].coordenadas.push({
      autofecha: formattedAutofecha,
      ilat: row.ilat,
      ilog: row.ilog,
    });
  });

  // Devolver la respuesta en formato JSON
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(response));
}

// Función para formatear la fecha
function formatFecha(isoString) {
  const date = new Date(isoString);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0'); // Mes empieza en 0
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}




async function obtenerHorasCadetePorFecha(connection, data, res, tableName) {
  const query = `SELECT * FROM ${tableName} WHERE didempresa = ? AND cadete = ? AND DATE(autofecha) = ?`;
  const [results] = await connection.execute(query, [data.didempresa, data.cadete, data.fecha]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results));
}

// Endpoint POST para recibir un JSON con clave "operador"
app.post('/consultas', async (req, res) => {
    const dataEntrada = req.body;
	const connection = await pool.getConnection();
	
	
    if (!dataEntrada.operador) {
        return res.status(400).json({ error: 'Falta la clave "operador" en el body' });
    }

    // Obtener la fecha actual en formato YYYY_MM_DD
    const fecha = new Date();
    const claveFechaRedis = `${fecha.getFullYear()}_${(fecha.getMonth() + 1).toString().padStart(2, '0')}_${fecha.getDate().toString().padStart(2, '0')}`;
	const claveFechaDb = `gps_${fecha.getDate().toString().padStart(2, '0')}_${(fecha.getMonth() + 1).toString().padStart(2, '0')}_${fecha.getFullYear()}`;
	//console.log(claveFechaRedis);
	//console.log(claveFechaDb);
	
	//await redisClient.connect();
	
    try {
		console.log(dataEntrada.operador);
    
		if(dataEntrada.operador == "getActual"){		
			await getActualData(connection, dataEntrada, res,claveFechaDb);
		} else if (dataEntrada.operador == "getHistorial"){
  
			await getHistorial(connection, dataEntrada, res, claveFechaDb);		
      	
		} else if (dataEntrada.operador == "getAll"){
			await getAll(connection, dataEntrada, res, claveFechaDb);
      
		} else if (dataEntrada.operador == "cadeteFiltrado"){
     
   
      
			await obtenerHorasCadetesPorFecha(connection, dataEntrada, res, claveFechaDb);
		} else if (dataEntrada.operador == "cadeteFiltradoUnico"){
			await obtenerHorasCadetePorFecha(connection, dataEntrada, res , claveFechaDb);
		}

    } catch (error) {
        console.error('Error obteniendo datos de Redis:', error);
        res.status(500).json({ error: 'Error interno del servidor' });
    } finally {
		connection.release();
	}
});
app.get('/', async (req, res) => {
  res.status(200).json({
    estado: true,
    mesanje: "Hola chris"
});
});

// Iniciar el servidor
app.listen(port, () => {
    console.log(`Servidor corriendo en http://localhost:${port}`);
});

