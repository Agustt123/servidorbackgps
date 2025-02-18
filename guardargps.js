const amqp = require('amqplib');
const redis = require('redis');
const axios = require('axios'); // Para manejar solicitudes HTTP
const { exec } = require('child_process');

const pm2 = require('pm2'); // Importar PM2
class EnvioProcessor {
  constructor() {
    this.token = null;
    this.dataEnvioML = null;
    this.dataRedisEnvio = null;
    this.dataRedisFecha = null;
    this.sellerid = null;
    this.shipmentid = null;
    this.clave = null;
  }

  setClave(claveabuscar) {
    this.clave = claveabuscar;
  }

  setSellerid(sellerid) {
    this.sellerid = sellerid;
  }

  setShipmentid(shipmentid) {
    this.shipmentid = shipmentid;
  }

  setToken(token) {
    this.token = token;
  }

  setDataEnvioML(dataEnvioML) {
    this.dataEnvioML = dataEnvioML;
  }

  setDataRedisEnvio(dataRedisEnvio) {
    this.dataRedisEnvio = dataRedisEnvio;
  }

  setDataRedisFecha(dataRedisFecha) {
    if (dataRedisFecha != "") {
      this.dataRedisFecha = JSON.parse(dataRedisFecha);
    }
  }

  
  async actualizaFechasRedis(ml_clave, ml_fechas, ml_estado, ml_subestado) {
    let data = this.dataRedisFecha;
    data.fecha = ml_fechas;
    data.clave = ml_estado + "-" + ml_subestado;
    data.estado = ml_estado;
    data.subestado = ml_subestado;

    if (!redisClient.isOpen) await redisClient.connect();
    await redisClient.hDel("estadoFechasML", this.clave);
    await redisClient.hSet("estadoFechasML", this.clave, JSON.stringify(data));
  }

  async eliminarCLavesEntregadosYCancelados() {
    if (!redisClient.isOpen) await redisClient.connect();
    await redisClient.hDel("estadoFechasML", this.clave);
    await redisClient.hDel("estadosEnviosML", this.clave);
  }

  async obtenerFechaFinalyestado() {
    const ml_fechas = this.dataEnvioML.status_history;
    const ml_estado = this.dataEnvioML.status;
    const ml_subestado = this.dataEnvioML.substatus;

    let estadonumero = -1;
    let fecha = null;

    if (ml_estado === "delivered") {
      estadonumero = 5;
      fecha = ml_fechas.date_delivered;
    } else if (ml_estado === "cancelled") {
      estadonumero = 8;
      fecha = ml_fechas.date_cancelled;
    } else if (ml_estado === "shipped") {
      estadonumero = 2;
      fecha = ml_fechas.date_shipped;

      if (ml_subestado === "delivery_failed" || ml_subestado === "receiver_absent") {
        estadonumero = 6;
        fecha = ml_fechas.date_first_visit;
      } else if (ml_subestado === "claimed_me") {
        estadonumero = 8;
        fecha = ml_fechas.date_first_visit;
      } else if (ml_subestado === "buyer_rescheduled") {
        estadonumero = 12;
        fecha = ml_fechas.date_first_visit;
      }
    } else if (ml_estado === "not_delivered") {
      estadonumero = 8;
      fecha = ml_fechas.date_returned;
    }

    return { estado: estadonumero, fecha: fecha };
  }

  async sendToServerEstado(dateE) {
    try {
      // Establecer conexión con RabbitMQ
      if (!this.channel) {
        this.connection = await amqp.connect({
          protocol: 'amqp',
          hostname: '158.69.131.226',
          port: 5672,
          username: 'lightdata',
          password: 'QQyfVBKRbw6fBb',
          heartbeat: 30,
        });
        this.channel = await this.connection.createChannel();
        await this.channel.assertQueue("srvshipmltosrvstates", {
          durable: true,
        });
      }

      const message = typeof dateE === 'string' ? dateE : JSON.stringify(dateE);
      this.channel.sendToQueue("srvshipmltosrvstates", Buffer.from(message), {
        persistent: true,
      });

      setTimeout(() => {
        this.channel.close();
        this.connection.close();
      }, 500);
    } catch (error) {
      console.error('Error al enviar el mensaje:', error);
    }
  }

  async actualizoDataRedis() {
    const fechaact = await getCurrentDateInArgentina();
    this.dataRedisEnvio.fechaActualizacion = fechaact;
    if (!redisClient.isOpen) await redisClient.connect();
    await redisClient.hDel("estadosEnviosML", this.clave);
    await redisClient.hSet("estadosEnviosML", this.clave, JSON.stringify(this.dataRedisEnvio));
  }

  async procesar() {
    if (!this.token || !this.dataEnvioML || !this.dataRedisEnvio) {
      console.error("Faltan datos para procesar el envío.");
      return { status: "error", message: "Faltan datos para procesar el envío." };
    }

    if (!this.dataRedisFecha) {
      const ml_fechas = this.dataEnvioML.status_history;
      const ml_estado = this.dataEnvioML.status;
      const ml_subestado = this.dataEnvioML.substatus;
      const ml_clave = `${ml_estado}-${ml_subestado}`;

      this.dataRedisFecha = {
        fecha: ml_fechas,
        clave: ml_clave,
        estado: ml_estado,
        subestado: ml_subestado,
      };

      await this.actualizaFechasRedis(ml_clave, ml_fechas, ml_estado, ml_subestado);
    }

    await this.actualizoDataRedis();

    const response = await this.obtenerFechaFinalyestado();
    const estadonumero = response.estado;
    let fecha = response.fecha;

    fecha = await convertToArgentinaTime(fecha);

    const dataE = {
      didempresa: this.dataRedisEnvio.didEmpresa,
      didenvio: this.dataRedisEnvio.didEnvio,
      estado: estadonumero,
      subestado: this.dataEnvioML.substatus,
      estadoML: this.dataEnvioML.status,
      fecha: fecha,
      quien: 0,
    };

    await this.sendToServerEstado(dataE);

    if (estadonumero === 5 || estadonumero === 8) {
      await this.eliminarCLavesEntregadosYCancelados();
    }

    return {
      status: "success",
      message: "Envío procesado correctamente",
    };
  }
}
async function reiniciarScript() {
  return new Promise((resolve, reject) => {
    pm2.connect((err) => {
      if (err) {
        console.error('Error al conectar a PM2:', err);
        return reject(err);
      }

      pm2.restart('serverng.js', (err) => {
        pm2.disconnect(); // Desconectar de PM2
        if (err) {
          console.error('Error al reiniciar el script:', err);
          return reject(err);
        }
        console.log('Script reiniciado correctamente.');
        resolve();
      });
    });
  });
}
let Atokens = [];
const claveEstadoRedis = 'estadosEnviosML';
const claveEstadoFechasML = 'estadoFechasML';

const redisClient = redis.createClient({
  socket: {
    host: '192.99.190.137',
    port: 50301,
  },
  password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg',
});

redisClient.on('error', (err) => {
  console.error('Error al conectar con Redis:', err);
});

async function main() {
  try {
    await redisClient.connect();
    await getTokenRedis();
    await consumirMensajes();
  } catch (error) {
    await reiniciarScript();
    console.error('Error en la ejecución principal:', error);
  } finally {
    await redisClient.disconnect();
  }
}

async function getCurrentDateInArgentina() {
  const now = new Date();
  const argentinaOffset = -3 * 60 * 60 * 1000;
  const argentinaTime = new Date(now.getTime() + argentinaOffset);

  const year = argentinaTime.getFullYear();
  const month = String(argentinaTime.getMonth() + 1).padStart(2, '0');
  const day = String(argentinaTime.getDate()).padStart(2, '0');
  const hours = String(argentinaTime.getHours()).padStart(2, '0');
  const minutes = String(argentinaTime.getMinutes()).padStart(2, '0');
  const seconds = String(argentinaTime.getSeconds()).padStart(2, '0');

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

async function convertToArgentinaTime(dateString) {
  const date = new Date(dateString);
  const utcTime = date.getTime();
  const argentinaOffset = -3 * 60 * 60 * 1000;
  const argentinaTime = new Date(utcTime + argentinaOffset);
  const formattedDate = argentinaTime.toISOString().slice(0, 19).replace('T', ' ');
  return formattedDate;
}

async function obtenerDatosEnvioML(shipmentid, token) {
  try {
    const url = `https://api.mercadolibre.com/shipments/${shipmentid}`;
    const response = await axios.get(url, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    if (response.data && response.data.id) {
      return response.data;
    } else {
      console.error(`No se encontraron datos válidos para el envío ${shipmentid}.`);
      return null;
    }
  } catch (error) {
    console.error(`Error al obtener datos del envío ${shipmentid} desde Mercado Libre:`, error.message);
    return null;
  }
}

async function getTokenRedis() {
  try {
    const data = await redisClient.get('token');
    Atokens = JSON.parse(data || '{}');
  } catch (error) {
    console.error('Error al obtener tokens de Redis:', error);
  }
}

async function getTokenForSeller(seller_id) {
  try {
    if (!redisClient.isOpen) await redisClient.connect();
    const token = await redisClient.hGet('token', seller_id);

    if (token) {
      return token;
    } else {
      return null;
    }
  } catch (error) {
    console.error('Error al obtener el token de Redis:', error);
    return null;
  }
}

function extractKey(resource) {
  const match = resource.match(/\/shipments\/(\d+)/);
  return match ? match[1] : null;
}




async function consumirMensajes() {
    let connection;
    let channel;
    let retryCount = 0;
    const maxRetries = 2; // Límite de intentos
    const scriptName = 'serverng.js'; // Cambia esto por el nombre de tu script

    const reconnect = async () => {
        try {
            // Verifica si la conexión y el canal están abiertos antes de cerrarlos
            if (connection && connection.isOpen) await connection.close();
            if (channel && channel.isOpen) await channel.close();

            // Conectar a RabbitMQ
            connection = await amqp.connect({
                protocol: 'amqp',
                hostname: '158.69.131.226',
                port: 5672,
                username: 'lightdata',
                password: 'QQyfVBKRbw6fBb',
                heartbeat: 30,
            });

            // Crear un nuevo canal
            channel = await connection.createChannel();
            await channel.assertQueue('shipments_states_callback_ml', { durable: true });

            // Consumir mensajes
            channel.consume('shipments_states_callback_ml', async (mensaje) => {
                if (mensaje) {
                    const data = JSON.parse(mensaje.content.toString());
                    const shipmentid = extractKey(data['resource']);
                    const sellerid = data['sellerid'];
                    const claveabuscar = `${sellerid}-${shipmentid}`;

                    if (!redisClient.isOpen) await redisClient.connect();
                    const exists = await redisClient.hExists(claveEstadoRedis, claveabuscar);

                    if (exists) {
                        let envioData = await redisClient.hGet(claveEstadoRedis, claveabuscar);
                        envioData = JSON.parse(envioData);
                        const token = await getTokenForSeller(sellerid);

                        if (!token) return;

                        const envioML = await obtenerDatosEnvioML(shipmentid, token);

                        if (!envioML) return;

                        let envioRedisFecha = await redisClient.hGet(claveEstadoFechasML, claveabuscar);
                        if (!envioRedisFecha) {
                            envioRedisFecha = JSON.stringify({
                                fecha: envioML.status_history,
                                clave: `${envioML.status}-${envioML.substatus}`,
                                estado: envioML.status,
                                subestado: envioML.substatus,
                            });
                        }

                        const envio = new EnvioProcessor();
                        envio.setToken(token);
                        envio.setSellerid(sellerid);
                        envio.setShipmentid(shipmentid);
                        envio.setClave(claveabuscar);
                        envio.setDataEnvioML(envioML);
                        envio.setDataRedisEnvio(envioData);
                        envio.setDataRedisFecha(envioRedisFecha);
                        const resultado = await envio.procesar();
                    }
                    channel.ack(mensaje);
                }
            }, { noAck: false });

            // Manejo de errores en el canal
            channel.on('error', handleError);
            channel.on('close', handleClose);

            // Manejo de errores en la conexión
            connection.on('error', handleError);
            connection.on('close', handleClose);

            retryCount = 0; // Reiniciar el contador de reintentos cuando la reconexión tiene éxito
        } catch (err) {
            console.error('Error al conectar a RabbitMQ:', err);
            handleReconnect();
        }
    };

    const handleError = (err) => {
        console.error('Error:', err);
        handleReconnect();
    };

    const handleClose = () => {
        console.error('Conexión cerrada. Intentando reconectar...');
        handleReconnect();
    };

    const handleReconnect = () => {
        if (retryCount < maxRetries) {
            retryCount++;
            setTimeout(reconnect, 5000); // Reintentar después de 5 segundos
        } else {
            console.error('Se alcanzó el límite de reintentos de conexión. Reiniciando el script con PM2...');
            exec(`pm2 restart ${scriptName}`, (error, stdout, stderr) => {
                if (error) {
                    console.error(`Error al reiniciar el script: ${error.message}`);
                    return;
                }
                if (stderr) {
                    console.error(`Error en stderr: ${stderr}`);
                    return;
                }
                console.log(`Script reiniciado: ${stdout}`);
            });
        }
    };

    await reconnect();
}

  

async function simular() {
  let data = { "resource": "/shipments/44416729582", "sellerid": "179907718" };
  const shipmentid = extractKey(data['resource']);
  const sellerid = data['sellerid'];
  const claveabuscar = `${sellerid}-${shipmentid}`;
  if (!redisClient.isOpen) await redisClient.connect();
  let exists = await redisClient.hExists(claveEstadoRedis, claveabuscar);
  console.log('Nuevo envío recibido:', data);

  if (exists) {
    let envioData = await redisClient.hGet(claveEstadoRedis, claveabuscar);
    envioData = JSON.parse(envioData);
    const token = await getTokenForSeller(sellerid);
    const envioML = await obtenerDatosEnvioML(shipmentid, token);
    let envioRedisFecha = "";

    let exists = await redisClient.hExists(claveEstadoFechasML, claveabuscar);
    if (exists) {
      envioRedisFecha = await redisClient.hGet(claveEstadoFechasML, claveabuscar);
    }

    console.log("envioRedisFecha", envioRedisFecha);

    if (token) {
      const envio = new EnvioProcessor();
      envio.setToken(token);
      envio.setSellerid(sellerid);
      envio.setShipmentid(shipmentid);
      envio.setClave(claveabuscar);
      envio.setDataEnvioML(envioML);
      envio.setDataRedisEnvio(envioData);
      envio.setDataRedisFecha(envioRedisFecha);
      const resultado = await envio.procesar();
      console.log("Resultado final:", resultado);
      process.exit(0);
    }

    if (token) {
      console.log('Procesando con el token:', token);
      await updateFechaEstadoML(sellerid, shipmentid, envioML, envioData);
      process.exit(0);
    } else {
      console.log(`No se encontró token para seller_id ${sellerid}`);
    }
  } else {
    console.log(`Clave ${claveabuscar} no encontrada en Adata.`);
  }
}

// Llamar a la función principal
main();

