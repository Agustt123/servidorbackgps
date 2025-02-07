const express = require('express');







// Configuración del servidor Express
const app = express();






app.get('/', (req, res) => {
  res.status(200).json({
    estado: true,
    mesanje: "Hola chris"
});
});




const PORT = 13500;




(async () => {
  try {
 
 
    // Iniciar servidor
    app.listen(PORT, () => {
      console.log(`Servidor escuchando en http://localhost:${PORT}`);
    });
    
   
    process.on('SIGINT', async () => {
      console.log('Cerrando servidor...');
   
      process.exit();
    });
  } catch (err) {
    console.error('Error al iniciar el servidor:', err);
  }
})();
