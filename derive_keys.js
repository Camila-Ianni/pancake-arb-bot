require('dotenv').config();
const { ClobClient } = require('@polymarket/clob-client');

async function deriveKeys() {
    const privateKey = process.env.PRIVATE_KEY;
    if (!privateKey) {
        console.error("❌ ERROR: PRIVATE_KEY no encontrada en .env");
        return;
    }

    console.log("Generando credenciales oficiales desde Polymarket...");

    try {
        const { ethers } = require("ethers");
        const wallet = new ethers.Wallet(privateKey.startsWith('0x') ? privateKey : '0x' + privateKey);

        // Inicializar cliente con la llave L1 (wallet)
        const client = new ClobClient(
            "https://clob.polymarket.com",
            137,
            wallet
        );

        // Derive L2 API keys
        const creds = await client.deriveApiKey();

        console.log("\n✅ CREDS RECUPERADAS:\n");
        console.log(creds);
        console.log(`POLYMARKET_API_KEY=${creds.apiKey || creds.key || creds.id}`);
        console.log(`POLYMARKET_SECRET=${creds.secret}`);
        console.log(`POLYMARKET_PASSPHRASE=${creds.passphrase}`);
        console.log("\nCopia esto en tu archivo .env");
    } catch (e) {
        console.error("❌ ERROR generando llaves:", e.message);
        if (e.response && e.response.data) {
            console.error("Detalle:", e.response.data);
        }
    }
}

deriveKeys();
