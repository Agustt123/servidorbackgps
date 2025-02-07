const express = require('express');


const cors = require('cors');





// Configuración del servidor Express
const app = express();

app.use(cors({
  origin: '*', // Permite solo este origen
  methods: ['GET', 'POST'], // Limitar los métodos HTTP
  allowedHeaders: ['Content-Type'], // Permitir ciertos encabezados
}));





app.get('/', (req, res) => {
  res.status(200).json({
    estado: true,
    mesanje: "Hola chris"
});
});




const PORT = 13000;




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