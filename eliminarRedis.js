const { redisClient } = require("./dbconfig");

async function deleteOldRecords() {
  try {
    // Obtener la fecha de hace 7 días
    const sevenDaysAgo = new Date(
      new Date().getTime() - 7 * 24 * 60 * 60 * 1000
    );
    const sevenDaysAgoStr = sevenDaysAgo
      .toISOString()
      .slice(0, 10)
      .replace(/-/g, "");

    // Obtener todas las claves de Redis
    const keys = await redisClient.keys("BACKGPS:*");

    // Iterar sobre las claves y eliminar los datos más antiguos
    for (const key of keys) {
      const [, empresa, fechaStr, cadete] = key.split(":");
      if (fechaStr < sevenDaysAgoStr) {
        await redisClient.del(key);
        console.log(
          `Eliminados los datos de la empresa ${empresa}, cadete ${cadete} con fecha ${fechaStr}`
        );
      }
    }

    console.log("Eliminación de datos antiguos completada.");
  } catch (error) {
    console.error("Error al eliminar los datos antiguos:", error);
  } finally {
    await redisClient.quit();
  }
}

// Ejecutar la función cada 24 horas
setInterval(deleteOldRecords, 24 * 60 * 60 * 1000);
