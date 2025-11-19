//require('dotenv').config();
const express = require("express");
const mysql = require("mysql2/promise");
const redis = require("redis");
const amqp = require("amqplib");
const cors = require("cors");
const crypto = require("crypto");
const { redisClient } = require("./dbconfig");
const { enviarCorreo } = require("./mail");
const { get } = require("http");

let connection;
let channel;
const app = express();
const port = process.env.PORT || 13000;

const rabbitMQUrl = "amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672";
const queue = "gps";

app.use(express.json()); // Middleware para parsear JSON
app.use(cors());
// Configuración de la base de datos
const pool = mysql.createPool({
  host: "10.70.0.67",
  user: "backgos",
  password: "pt25pt26pt",
  database: "gpsdata",

  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
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
        host: "192.99.190.137", // IP interna
        port: 50301, // Puerto interno
      },
      password: "sdJmdxXC8luknTrqmHceJS48NTyzExQg", // Contraseña para autenticación
    });

    // Manejo de errores de Redis
    redisClient.on("error", (err) => {
      console.error("Error al conectar con Redis:", err);
    });

    redisClient.on("connect", () => {
      // console.log('Conectado a Redis correctamente');
    });
  } catch (error) {
    console.error("Error conectando a la base de datos:", error);
    process.exit(1); // Salir de la aplicación si no se puede conectar
  }
})();


async function initRabbitMQ() {
  try {
    if (!connection || connection.connection.stream.destroyed) {
      connection = await amqp.connect(rabbitMQUrl);
      //   console.log("conecion creada 2 ");
      connection.on("error", (err) => {
        console.error("❌ Error en la conexión RabbitMQ:", err.message);
        connection = null;
      });

      connection.on("close", () => {
        console.warn("⚠️ Conexión RabbitMQ cerrada. Intentando reconectar...");
        connection = null;
        channel = null;
        setTimeout(initRabbitMQ, 2000); // Reintenta en 2 segundos
      });

      channel = await connection.createChannel();
      channel.on("error", (err) => {
        console.error("❌ Error en el canal RabbitMQ:", err.message);
        channel = null;
      });

      channel.on("close", () => {
        console.warn("⚠️ Canal RabbitMQ cerrado.");
        channel = null;
      });

      await channel.assertQueue(queue, { durable: true });
      console.log("✅ Conectado a RabbitMQ y canal creado");
    }
  } catch (err) {
    console.error("❌ Error inicializando RabbitMQ:", err.message);
    setTimeout(initRabbitMQ, 2000); // Reintenta en 2 segundos
  }
}

async function sendToRabbitMQ(data) {
  try {
    await initRabbitMQ();

    if (!channel) {
      throw new Error("No hay canal disponible para enviar el mensaje.");
    }

    channel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), {
      persistent: true,
    });

    if (data.empresa == 270) {
      //     console.log("📡 Mensaje enviado:", data);
    }
  } catch (error) {
    console.error("❌ Error al enviar mensaje a RabbitMQ:", error.message);
  }
}

process.on("exit", async () => {
  if (channel) await channel.close();
  if (connection) await connection.close();
  //  console.log("🔌 Conexión a RabbitMQ cerrada");
});
async function getActualData(connection, data, res, tableName) {
  const query = `SELECT ilat, ilog, bateria, velocidad, DATE_FORMAT(autofecha, '%d/%m/%Y %H:%i') as autofecha 
                 FROM ${tableName} 
                 WHERE didempresa = ? 
                   AND cadete = ? 
                   AND superado = 0  
                   AND ilat IS NOT NULL AND ilog IS NOT NULL
                   AND ilat != 0 AND ilog != 0
                   AND ilat != '' AND ilog != ''
                 ORDER BY autofecha DESC 
                 LIMIT 5`;

  const [results] = await connection.execute(query, [
    data.empresa,
    data.cadete,
  ]);

  const validResult = results.find(
    (r) =>
      r.ilat !== null &&
      r.ilog !== null &&
      r.ilat !== 0 &&
      r.ilog !== 0 &&
      r.ilat !== "" &&
      r.ilog !== "" &&
      typeof r.ilat !== "undefined" &&
      typeof r.ilog !== "undefined"
  );

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(validResult ? [validResult] : []));
}
// al principio de tu archivo si no está importado

async function getActualData2(connection, data, res, tableName) {
  // 1. Obtener fecha actual en formato YYYY-MM-DD
  const today = new Date().toISOString().slice(0, 10); // '2025-05-22'

  // 2. Calcular hash SHA-256
  const expectedHash = crypto.createHash("sha256").update(today).digest("hex");

  // 3. Verificar el hash recibido
  if (!data.token || data.token !== expectedHash) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Hash inválido o no provisto" }));
    return;
  }

  // Consulta a la base de datos
  const query = `SELECT ilat, ilog, bateria, velocidad, DATE_FORMAT(autofecha, '%d/%m/%Y %H:%i') as autofecha 
                 FROM ${tableName} 
                 WHERE didempresa = ? 
                   AND cadete = ? 
                   AND superado = 0  
                   AND ilat IS NOT NULL AND ilog IS NOT NULL
                   AND ilat != 0 AND ilog != 0
                   AND ilat != '' AND ilog != ''
                 ORDER BY autofecha DESC 
                 LIMIT 5`;

  const [results] = await connection.execute(query, [
    data.empresa,
    data.cadete,
  ]);

  const validResult = results.find(
    (r) =>
      r.ilat !== null &&
      r.ilog !== null &&
      r.ilat !== 0 &&
      r.ilog !== 0 &&
      r.ilat !== "" &&
      r.ilog !== "" &&
      typeof r.ilat !== "undefined" &&
      typeof r.ilog !== "undefined"
  );

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(validResult ? [validResult] : []));
}

async function getHistorial(connection, data, res, tableName) {
  const query = `SELECT * FROM ${tableName} WHERE didempresa = ? AND cadete = ? AND ilat != 0 AND ilog != 0`;
  const [results] = await connection.execute(query, [
    data.empresa,
    data.cadete,
  ]);
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(results));
}

async function getAll(connection, data, res, tableName) {
  if (!data.token) {
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Token no provisto" }));
    return;
  }
  const now = new Date();
  now.setHours(now.getHours() - 3);
  const today = now.toISOString().slice(0, 10);

  const expectedHash = crypto.createHash("sha256").update(today).digest("hex");
  //const expectedHash = today;

  if (data.token.trim().toLowerCase() !== expectedHash.toLowerCase()) {
    console.log("expectedHash", expectedHash);
    res.writeHead(401, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        error: "Token inválido o no coincide con la fecha actual",
      })
    );
    return;
  }

  const query = `SELECT hora,bateria,cadete,didempresa,ilat,ilog,precision_gps,velocidad FROM ${tableName} 
                 WHERE superado = 0 
                   AND didempresa = ? 
                   AND ilat != 0 AND ilog != 0 
                   AND ilat != '' AND ilog != '' 
                   AND ilat IS NOT NULL AND ilog IS NOT NULL`;

  const [results] = await connection.execute(query, [data.didempresa]);

  // Usamos un objeto donde cada key es el cadete y el valor es el último registro encontrado
  const agrupadoPorCadete = {};

  for (const row of results) {
    const cadeteId = row.cadete;
    agrupadoPorCadete[cadeteId] = {
      ...row,
      autofechaNg: formatDate(row.hora),
    };
  }

  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(agrupadoPorCadete));
}
// Uso esperado:
// const result = await getAllById(connection, didCadete, didempresa, claveFechaDb);

async function getAllById(connection, didCadete, didempresa, tableName) {
  const now = new Date();
  const fiveMinAgo = new Date(now);
  fiveMinAgo.setMinutes(fiveMinAgo.getMinutes() - 10);

  const hourStart = new Date(now);
  hourStart.setMinutes(0, 0, 0);
  const lowerBound = fiveMinAgo < hourStart ? hourStart : fiveMinAgo;

  const query = `
    SELECT hora, bateria, cadete, didempresa, ilat, ilog, precision_gps
    FROM ${tableName}
    WHERE cadete = ?
      AND didempresa = ?
      AND ilat != 0 AND ilog != 0
      AND ilat != '' AND ilog != ''
      AND ilat IS NOT NULL AND ilog IS NOT NULL
      AND hora >= ? AND hora <= ?
    ORDER BY hora DESC
    LIMIT 1
  `;

  const [rows] = await connection.execute(query, [
    didCadete,
    didempresa,
    lowerBound,
    now,
  ]);

  if (!rows.length) return null;

  const row = rows[0];

  // 👇 Solo hora y minuto en Buenos Aires (HH:mm)
  const hora = new Date(row.hora).toLocaleTimeString("es-AR", {
    timeZone: "America/Argentina/Buenos_Aires",
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });

  return {
    ilat: row.ilat,
    ilog: row.ilog,
    // lo dejás igual por compatibilidad
    hora,                           // <- nuevo campo "HH:mm"
  };
}


// Fecha/hora siempre en Buenos Aires: "YYYY-MM-DD HH:mm:ss"
function formatDateBA(dateString) {
  return new Date(dateString)
    .toLocaleString("sv-SE", {
      timeZone: "America/Argentina/Buenos_Aires",
      hour12: false,
    })
    .replace("T", " ");
}




// Función para formatear la fecha en "YYYY-MM-DD HH:MM:SS"
function formatDate(dateString) {
  const date = new Date(dateString);
  const offset = -3 * 60; // UTC-3 para Argentina
  date.setMinutes(date.getMinutes() + offset);

  return date.toISOString().slice(0, 19).replace("T", " ");
}

// Función para formatear la fecha en "YYYY-MM-DD HH:MM:SS"
function formatDate(dateString) {
  const date = new Date(dateString);
  const offset = -3 * 60; // UTC-3 para Argentina
  date.setMinutes(date.getMinutes() + offset);

  return date.toISOString().slice(0, 19).replace("T", " ");
}

async function obtenerHorasCadetesPorFecha(connection, data, res) {
  //  console.log("aaa");

  // Obtener la fecha en formato YYYY-MM-DD
  const fecha = data.fecha; // Por ejemplo, "2025-02-05"
  const [year, month, day] = fecha.split("-");

  // Generar el nombre de la tabla sin espacios
  const claveFechadb = `gps_${day}_${month}_${year}`; // Esto debe ser gps_05_02_2025

  // Verificar el nombre de la tabla
  //console.log(`Nombre de la tabla: '${claveFechadb}'`); // Asegúrate de que no haya espacios

  // Modificar la consulta para extraer solo la parte de la fecha de autofecha
  const query = `SELECT * FROM ${claveFechadb} WHERE didempresa = ? AND autofecha between ? and ? `;

  //console.log(query); // Asegúrate de que se imprima correctamente el nombre de la tabla
  const [results] = await connection.execute(query, [
    data.didempresa,
    `${data.fecha} ${data.horaDesde}:00`,
    `${data.fecha} ${data.horaHasta}:00`,
  ]);

  // Estructurar la respuesta
  const response = {};

  // Agrupar resultados por empresa
  results.forEach((row) => {
    const empresaId = row.didempresa; // Suponiendo que este es el ID de la empresa
    const choferId = row.cadete;
    // Suponiendo que este es el ID del chofer

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
      precision_gps: row.precision_gps,
      idDispositovo: row.idDispositivo,
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
  const month = String(date.getMonth() + 1).padStart(2, "0"); // Mes empieza en 0
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

async function obtenerHorasCadetePorFecha(connection, data, res, tableName) {
  // Obtener la fecha en formato YYYY-MM-DD

  console.log(data);

  const fecha = data.fecha; // Por ejemplo, "2025-02-05"
  const [year, month, day] = fecha.split("-");

  // Generar el nombre de la tabla sin espacios
  const claveFechadb = `gps_${day}_${month}_${year}`; // Esto debe ser gps_05_02_2025
  const query = `SELECT * FROM ${claveFechadb} WHERE didempresa = ? AND cadete = ? AND autofecha > ?`;
  const [results] = await connection.execute(query, [
    data.didempresa,
    data.cadete,
    `${data.hora_desde}%`,
  ]);
  const response = {};

  // Agrupar resultados por empresa

  results.forEach((row) => {
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
      precision_gps: row.precision_gps,
      idDispositovo: row.idDispositivo,
    });
  });

  // Devolver la respuesta en formato JSON
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify(response));
}

async function obtenerrecorridocadete(connection, data, res) {
  const today = new Date().toISOString().slice(0, 10);
  const expectedHash = crypto.createHash("sha256").update(today).digest("hex");
  const today2 = new Date().toISOString();


  /* if (data.token.trim().toLowerCase() !== expectedHash.toLowerCase()) {
     res.writeHead(401, { "Content-Type": "application/json" });
     res.end(
       JSON.stringify({
         error: "Token inválido ",
       })
     );
     return;
   }*/

  const camposRequeridos = [
    "didempresa",
    "cadete",
    "fecha_desde",
    "hora_desde",
    "hora_hasta",
  ];

  const faltantes = camposRequeridos.filter(
    (campo) => data[campo] === undefined
  );

  if (faltantes.length > 0) {
    res.writeHead(400, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        error: `Faltan los siguientes campos: ${faltantes.join(", ")}`,
      })
    );
    return;
  }

  const [day, month, year] = data.fecha_desde.split("/"); // "10/04/2025"
  const fechaKey = `${year}${month}${day}`; // "20250521"
  const redisKey = data.didempresa; // La empresa a consultar
  const cadete = data.cadete;

  // Si la empresa es 164, primero consulta Redis
  if (data.didempresa == "164") {
    try {
      const existingData = await redisClient.get("BACKGPS");
      if (existingData) {
        const estructura = JSON.parse(existingData);
        const cadeteData = estructura[redisKey]?.[fechaKey]?.[cadete];

        // Filtrar los datos por hora
        if (cadeteData) {
          const desde = new Date(
            `${year}-${month}-${day} ${data.hora_desde}:00`
          ).getTime();
          const hasta = new Date(
            `${year}-${month}-${day} ${data.hora_hasta}:00`
          ).getTime();
          //        console.log(`Desde: ${desde}, Hasta: ${hasta}`, "redisss");
          //
          const filteredData = cadeteData.filter((item) => {
            // Convertir el formato de Redis a Date
            const itemDate = new Date(
              `${item.fecha.slice(0, 4)}-${item.fecha.slice(
                4,
                6
              )}-${item.fecha.slice(6, 8)} ${item.fecha.slice(
                8,
                10
              )}:${item.fecha.slice(10, 12)}:${item.fecha.slice(12, 14)}`
            ).getTime();
            return itemDate >= desde && itemDate <= hasta;
          });

          if (filteredData.length > 0) {
            const response = {
              coordenadas: filteredData.map((item) => ({
                autofecha: item.fecha,
                precision_gps: item.precision_gps,
                idDispositovo: item.idDispositovo,
                ilat: item.latitud,
                ilog: item.longitud,
              })),
            };

            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(response));
            return;
          }
        }
      }
    } catch (error) {
      console.error("Error consultando Redis:", error);
      // Si alguna propiedad de Redis está en null, sigue adelante y consulta la base de datos
      //  console.log("Continuando con la consulta a la base de datos...");
    }
  }

  // Si no hay datos en Redis o no es empresa 164, consulta a la base de datos
  const claveFechadb = `gps_${day}_${month}_${year}`;
  const fechaFormateada = `${year}/${month}/${day}`; // "2025-04-10"

  const query = `
    SELECT * FROM ${claveFechadb} 
    WHERE didempresa = ? 
      AND cadete = ? 
      AND autofecha BETWEEN ? AND ? and ilat != 0 and ilog != 0
  `;

  const desde = `${fechaFormateada} ${data.hora_desde}:00`;
  const hasta = `${fechaFormateada} ${data.hora_hasta}:00`;

  try {
    const [results] = await connection.execute(query, [
      data.didempresa,
      data.cadete,
      desde,
      hasta,
    ]);

    const response = {
      coordenadas: [],
    };

    results.forEach((row) => {
      const formattedAutofecha = formatFecha(row.autofecha);
      response.coordenadas.push({
        autofecha: formattedAutofecha,
        precision_gps: row.precision_gps,
        idDispositovo: row.idDispositivo,
        ilat: row.ilat,
        ilog: row.ilog,
      });
    });

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(response));
  } catch (error) {
    console.error("Error ejecutando la consulta:", error);
    res.writeHead(500, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Error al ejecutar la consulta SQL" }));
  }
}

async function checkCadete(connection, data) {
  const query = `SELECT * FROM gps_${data.fecha}  WHERE didempresa = ? AND cadete = ?`;
  const [results] = await connection.execute(query, [
    data.didempresa,
    data.cadete,
  ]);
  return results.length > 0;
}

async function cadetesActivo(connection, data) {
  const query = `SELECT cadete FROM gps_${data.fecha} WHERE didempresa = ?`;
  const [results] = await connection.execute(query, [data.didempresa]);

  // Usamos un Set para evitar duplicados
  const cadetesSet = new Set();

  for (const row of results) {
    if (row.cadete) {
      cadetesSet.add(row.cadete);
    }
  }

  // Convertimos el Set a array y luego a string separado por coma
  const cadetesUnicos = Array.from(cadetesSet).join(',');

  return cadetesUnicos;
}

app.post("/check", async (req, res) => {
  const data = req.body;
  const connection = await pool.getConnection();

  if (!data.didempresa || !data.cadete) {
    connection.release();
    return res.status(400).json({
      error: "Faltan datos requeridos (didempresa o cadete)",
    });
  }

  try {
    const existe = await checkCadete(connection, data);
    connection.release();

    if (existe) {
      return res.status(200).json({ existe: true });
    } else {
      return res.status(200).json({ existe: false });
    }
  } catch (error) {
    console.error("Error al verificar cadete:", error);
    connection.release();
    return res
      .status(500)
      .json({ ok: false, error: "Error interno del servidor" });
  }
});

// Endpoint POST para recibir un JSON con clave "operador"
app.post("/consultas", async (req, res) => {
  const dataEntrada = req.body;
  const connection = await pool.getConnection();

  if (!dataEntrada.operador) {
    return res
      .status(400)
      .json({ error: 'Falta la clave "operador" en el body' });
  }

  // Obtener la fecha actual en formato YYYY_MM_DD
  const fecha = new Date();
  const claveFechaRedis = `${fecha.getFullYear()}_${(fecha.getMonth() + 1)
    .toString()
    .padStart(2, "0")}_${fecha.getDate().toString().padStart(2, "0")}`;
  const claveFechaDb = `gps_${fecha.getDate().toString().padStart(2, "0")}_${(
    fecha.getMonth() + 1
  )
    .toString()
    .padStart(2, "0")}_${fecha.getFullYear()}`;
  //console.log(claveFechaRedis);
  //console.log(claveFechaDb);

  //await redisClient.connect();

  try {
    //console.log(dataEntrada.operador);

    if (dataEntrada.operador == "getActual") {
      await getActualData(connection, dataEntrada, res, claveFechaDb);
    } else if (dataEntrada.operador == "getHistorial") {
      await getHistorial(connection, dataEntrada, res, claveFechaDb);
    } else if (dataEntrada.operador == "guardar") {
      let body = "";

      req.on("data", (chunk) => {
        body += chunk.toString();
      });

      req.on("end", () => {
        dataEntrada = JSON.parse(body); // Parseamos el JSON recibido

        const todayToken = `${year}${month}${day}`;
        if (1 == 1) {
          // Si decides validar el token, cambia esta línea

          sendToRabbitMQ(dataEntrada);

          // Responder al cliente (opcional)
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ status: "true" }));
        }
      });
    } else if (dataEntrada.operador == "getAll") {
      await getAll(connection, dataEntrada, res, claveFechaDb);
    } else if (dataEntrada.operador == "cadeteFiltrado") {
      await obtenerHorasCadetesPorFecha(
        connection,
        dataEntrada,
        res,
        claveFechaDb
      );
    } else if (dataEntrada.operador == "cadeteFiltradoUnico") {
      await obtenerHorasCadetePorFecha(
        connection,
        dataEntrada,
        res,
        claveFechaDb
      );
    } else if (dataEntrada.operador == "recorridoCadete") {
      await obtenerrecorridocadete(connection, dataEntrada, res);
    }
  } catch (error) {
    console.error("Error obteniendo datos de Redis:", error);
    res.status(500).json({ error: "Error interno del servidor" });
  } finally {
    connection.release();
  }
});

app.post("/actualizarlatlog", async (req, res) => {
  const dataEntrada = req.body;
  const connection = await pool.getConnection();

  try {
    if (!dataEntrada.fecha || !dataEntrada.didempresa || !dataEntrada.cadete) {
      return res.status(400).json({
        error: "Faltan datos requeridos (fecha, didempresa o cadete)",
      });
    }

    const [fecha, hora] = dataEntrada.fecha.split(" ");
    const [anio, mes, dia] = fecha.split("-");
    const [hh, mm, ss] = hora.split(":");
    const tableName = `gps_${dia}_${mes}_${anio}`;

    // Crear fecha local (sin UTC)
    const fechaOriginal = new Date(anio, parseInt(mes) - 1, dia, hh, mm, ss);
    const diezMinAntes = new Date(fechaOriginal.getTime() - 10 * 60 * 1000);
    const diezMinDespues = new Date(fechaOriginal.getTime() + 10 * 60 * 1000);

    const toMySQLDateTime = (date) => {
      const pad = (n) => n.toString().padStart(2, "0");
      return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
        date.getDate()
      )} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(
        date.getSeconds()
      )}`;
    };

    const desde = toMySQLDateTime(diezMinAntes);
    const hasta = toMySQLDateTime(diezMinDespues);

    const [tablas] = await connection.query(`SHOW TABLES LIKE ?`, [tableName]);
    if (tablas.length === 0) {
      return res.status(404).json({ error: `La tabla ${tableName} no existe` });
    }

    const query = `
      SELECT ilat, ilog
      FROM \`${tableName}\`
      WHERE cadete = ?
        AND didempresa = ?
      
  
        AND hora BETWEEN ? AND ?
          ORDER BY ABS(TIMESTAMPDIFF(SECOND, hora, ?)) ASC
  LIMIT 1
    `;

    const [result] = await connection.execute(query, [
      dataEntrada.cadete,
      dataEntrada.didempresa,
      desde,
      hasta,
      dataEntrada.fecha,
    ]);

    //  console.log({ tableName, desde, hasta, result });

    res.status(200).json({
      message: "Consulta exitosa",
      result: result[0] || { ilat: 0, ilog: 0 },
    });
  } catch (error) {
    console.error("Error al actualizar:", error);
    res.status(500).json({ error: "Error interno del servidor" });
  } finally {
    connection.release();
  }
});
app.post("/backgps", async (req, res) => {
  const data = {
    ...req.body,
    operador: "guardar",
  };
  //console.log(data, "data del viejo ");

  try {
    await sendToRabbitMQ(data);
    res.status(200).json({ ok: true, mensaje: "Mensaje enviado a RabbitMQ" });
  } catch (error) {
    console.error("❌ Error al enviar mensaje:", error);
    res.status(500).json({ ok: false, error: "No se pudo enviar a RabbitMQ" });
  }
});
app.post("/check", async (req, res) => {
  const data = req.body;
  const connection = await pool.getConnection();
  // console.log(data);

  if (!data.didempresa || !data.cadete) {
    return res.status(400).json({
      error: "Faltan datos requeridos (didempresa o cadete)",
    });
  }
  try {
    //  console.log("entre");

    await checkCadete(connection, data, res);
  } catch (error) {
    console.error("Error al enviar mensaje:", error);
    res.status(500).json({ ok: false, error: "No se pudo enviar a RabbitMQ" });
  }
  //console.log(data, "data del viejo ");

  try {
    res.status(200).json({ ok: true, mensaje: "Mensaje enviado a RabbitMQ" });
  } catch (error) {
    console.error("❌ Error al enviar mensaje:", error);
    res.status(500).json({ ok: false, error: "No se pudo enviar a RabbitMQ" });
  }
});

app.post("/test-connection", async (req, res) => {
  const { host, port, user, password } = req.body;

  let connection;
  try {
    connection = await mysql.createConnection({
      host,
      port,
      user,
      password,
    });

    // Test simple: ejecutar una query como "SELECT 1"
    await connection.query("SELECT 1");

    res.json({ success: true });
  } catch (error) {
    console.error("Error de conexión:", error.message);
    res.json({ success: false });
  } finally {
    if (connection) {
      await connection.end();
    }
  }
});

app.post("/enviar-mail", async (req, res) => {
  const { data, email, emailEmpresa } = req.body;

  if (!data || !email) {
    return res.status(400).json({ error: "Faltan campos requeridos" });
  }

  try {
    await enviarCorreo(data, email, emailEmpresa);
    res.json({ success: true, mensaje: "Correo enviado correctamente" });
  } catch (error) {
    console.error("❌ Error al enviar el correo:", error);
    res.status(500).json({ error: "No se pudo enviar el correo" });
  }
});

app.post("/choferesActivos", async (req, res) => {
  const dataEntrada = req.body;

  try {
    if (!dataEntrada.fecha || !dataEntrada.didempresa) {
      return res.status(400).json({
        error: "Faltan datos requeridos (fecha o didempresa)",
      });
    }

    const connection = await pool.getConnection();
    const cadetes = await cadetesActivo(connection, dataEntrada);
    connection.release();
    if (!cadetes) {
      return res.status(404).json({ error: "No se encontraron cadetes activos" });
    }
    res.status(200).json({ estado: true, cadetes: cadetes });

  } catch (error) {
    console.error("Error al obtener cadetes activos:", error);
    res.status(500).json({ error: "Error interno del servidor" });
  }
});

// Ejemplo de endpoint con ambos params en el path:
// GET /cadetes/:didempresa/:didCadete
app.get("/cadetes/:didempresa/:didCadete", async (req, res) => {

  const didempresa = req.params.didempresa;
  const didCadete = req.params.didCadete;

  if (!didempresa || !didCadete) {
    return res.status(400).json({ error: "didempresa y didCadete son requeridos" });
  }

  // armás la tabla del día como venías haciendo
  const fecha = new Date();
  const claveFechaDb = `gps_${fecha.getDate().toString().padStart(2, "0")}_${(fecha.getMonth() + 1)
    .toString()
    .padStart(2, "0")}_${fecha.getFullYear()}`;

  const connection = await pool.getConnection();
  try {

    const result = await getAllById(connection, didCadete, didempresa, claveFechaDb);

    if (!result) {
      return res.status(404).json({
        estado: false,
        error: "Sin datos para ese cadete en los últimos 10 minutos",
      });
    }

    res.status(200).json({ estado: true, cadete: result });
  } catch (error) {
    console.error("Error al procesar cadete:", error);
    res.status(error.status || 500).json({
      error: error.message || "Error interno del servidor",
    });
  } finally {
    connection.release();
  }
});

app.get("/ping", (req, res) => {
  const currentDate = new Date();
  currentDate.setHours(currentDate.getHours()); // Resta 3 horas

  // Formatear la hora en el formato HH:MM:SS
  const hours = currentDate.getHours().toString().padStart(2, "0");
  const minutes = currentDate.getMinutes().toString().padStart(2, "0");
  const seconds = currentDate.getSeconds().toString().padStart(2, "0");

  const formattedTime = `${hours}:${minutes}:${seconds}`;

  res.status(200).json({
    hora: formattedTime,
  });
});

// Iniciar el servidor
app.listen(port, () => {
  console.log(`Servidor corriendo en http://localhost:${port}`);
});
