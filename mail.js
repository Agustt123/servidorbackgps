const nodemailer = require("nodemailer");

/**
 * Envía un correo electrónico usando nodemailer.
 * @param {string} asunto - El asunto del correo.
 * @param {string} mensaje - El cuerpo del mensaje.
 * @param {string} destinatario - El email del destinatario.
 * @returns {Promise<void>}
 */
async function enviarCorreo(data, destinatario) {
  const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com", // o el SMTP de tu proveedor
    port: 587,
    secure: false,
    auth: {
      user: "lightdataargentina@gmail.com",
      pass: "sjexjcjixmesdjyv", // usar App Password si es Gmail
    },
  });

  await transporter.sendMail({
    from: `${data.empresa}`,
    to: destinatario,
    subject: data.asunto,
    text: data.texto,
    html: `<p>${data.texto}</p>`, // opcional: cuerpo en HTML
  });
}

module.exports = { enviarCorreo };
