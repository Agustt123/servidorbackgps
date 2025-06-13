const nodemailer = require("nodemailer");

/**
 * Envía un correo electrónico usando nodemailer.
 * @param {Object} data - Datos del correo (nombre, asunto, texto, emailEmpresa).
 * @param {string} destinatario - El email del destinatario principal.
 * @returns {Promise<void>}
 */
async function enviarCorreo(data, destinatario) {
  const transporter = nodemailer.createTransport({
    host: "smtp.gmail.com",
    port: 587,
    secure: false,
    auth: {
      user: "lightdataargentina@gmail.com",
      pass: "sjexjcjixmesdjyv", // usar App Password si es Gmail
    },
  });

  console.log(data, "dataaa");
  console.log(destinatario, "destinatario");


  // Crear lista de destinatarios
  const destinatarios = [destinatario];
  if (data.emailEmpresa) {
    destinatarios.push(data.emailEmpresa);
  }
  console.log(destinatarios, "destinatarios");

  await transporter.sendMail({
    from: `"${data.nombre}" <lightdataargentina@gmail.com>`,
    to: destinatarios.join(", "),
    subject: data.asunto,
    text: data.texto,
    html: `<p>${data.texto}</p>`,
  });
}

module.exports = { enviarCorreo };
