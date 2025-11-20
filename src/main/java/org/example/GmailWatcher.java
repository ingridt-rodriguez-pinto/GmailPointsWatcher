package org.example;

import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.model.CallbackQuery;
import com.pengrad.telegrambot.model.Update;
import com.pengrad.telegrambot.model.request.InlineKeyboardButton;
import com.pengrad.telegrambot.model.request.InlineKeyboardMarkup;
import com.pengrad.telegrambot.request.AnswerCallbackQuery;
import com.pengrad.telegrambot.request.EditMessageText;
import com.pengrad.telegrambot.request.GetUpdates;
import com.pengrad.telegrambot.request.SendMessage;
import jakarta.mail.*;
import jakarta.mail.internet.MimeMultipart;
import jakarta.mail.search.FlagTerm;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GmailWatcher {
    private static final String TELEGRAM_TOKEN = "";
    private static final String USERNAME = "";
    private static final String PASSWORD = "";
    private static final int TIMEOUT_SECONDS = 60;
    private static final long CHAT_ID = 0; // ID del usuario a quien mandar el mensaje
    private static double newCashBackTotal;
    private static double oldCashBackTotal;
    private final TelegramBot bot = new TelegramBot(TELEGRAM_TOKEN);
    private DecimalFormat df = new DecimalFormat("0.00");
    private int lastUpdateId = 0;

    public static void main(String[] args) {
        GmailWatcher cashbackBot = new GmailWatcher();
        //calculateOldCashBack();
        cashbackBot.startEmailCheck();
        cashbackBot.startListening();
    }

    private static void saveCashbackRates(JSONArray companiesArray) throws IOException {
        try (FileWriter file = new FileWriter("cashback_rates.json")) {
            file.write(companiesArray.toJSONString());
        }
    }

    // Carga los porcentajes de cada empresa en el json
    private static JSONArray loadCashbackRates() throws IOException, ParseException {
        if (!Files.exists(Paths.get("cashback_rates.json"))) {
            return new JSONArray(); // Si no existe el archivo,retorna un array vacio
        }
        JSONParser parser = new JSONParser();
        Object parsedData = parser.parse(new FileReader("cashback_rates.json"));

        if (parsedData instanceof JSONArray) {
            return (JSONArray) parsedData;
        } else if (parsedData instanceof JSONObject) {
            //Retorna el json
            JSONArray jsonArray = new JSONArray();
            jsonArray.add(parsedData);
            return jsonArray;
        } else {
            throw new ParseException(ParseException.ERROR_UNEXPECTED_TOKEN, "Invalid JSON structure");
        }
    }

    private static void calculateNewCashBack() throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        JSONObject parsedData = (JSONObject) parser.parse(new FileReader("registros.json"));
        newCashBackTotal = 0.00;
        // Loopea por cada empresa
        for (Object company : parsedData.keySet()) {
            JSONArray transactions = (JSONArray) parsedData.get(company);

            // Loopea por cada transaccion para sumar el cashback
            for (Object transactionObj : transactions) {
                JSONObject transaction = (JSONObject) transactionObj;
                double cashback = (double) transaction.get("cashback");
                newCashBackTotal += cashback;
            }
        }
    }

    private static void calculateOldCashBack() throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        JSONObject parsedData = (JSONObject) parser.parse(new FileReader("registros.json"));
        oldCashBackTotal = 0.00;
        // Loopea por cada empresa
        for (Object company : parsedData.keySet()) {
            JSONArray transactions = (JSONArray) parsedData.get(company);

            // Loopea por cada transaccion para sumar el cashback
            for (Object transactionObj : transactions) {
                JSONObject transaction = (JSONObject) transactionObj;
                double cashback = (double) transaction.get("cashback");
                oldCashBackTotal += cashback;
            }
        }
    }

    // Escucha el bot de telegram
    private void startListening() {
        new Thread(() -> {
            while (true) {
                try {
                    getUpdates();
                    Thread.sleep(500); //
                } catch (Exception e) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void getUpdates() throws IOException, ParseException {
        GetUpdates getUpdates = new GetUpdates().limit(100).offset(lastUpdateId).timeout(0);
        List<Update> updates = bot.execute(getUpdates).updates();
        System.out.println("Received updates: " + (updates == null ? "No updates" : updates.size()));
        if (updates != null) {
            for (Update update : updates) {
                lastUpdateId = update.updateId() + 1; // Incrementa el ID para el siguiente update
                if (update.callbackQuery() != null) {
                    handleTelegramCallback(update.callbackQuery());
                }
            }
        }

    }

    // Procesa el callback cuando el usuario selecciona una opcion en telegram
    private void handleTelegramCallback(CallbackQuery callbackQuery) throws IOException, ParseException {
        String selectedRate = callbackQuery.data(); // Seleccione 1,2 o 5
        String companyName = callbackQuery.message().text().substring(0, callbackQuery.message().text().indexOf(",")); //Extraigo el nombre de la empresa del mensaje de telegram
        String usdValue = callbackQuery.message().text().toString().substring(callbackQuery.message().text().indexOf("$")).replace("$", ""); //Extraigo el valor del mensaje de telegram
        Long chatId = callbackQuery.message().chat().id();
        Integer messageId = callbackQuery.message().messageId();
        double cashbackRate = Double.parseDouble(selectedRate) / 100.0;

        // Calcula el cashback y envia el mensaje
        double usdValueDouble = Double.parseDouble(df.format(Double.parseDouble(usdValue)));
        calculateAndSendCashback(companyName, usdValueDouble, cashbackRate);

        // Marca el mensaje como recibido y edita el mensaje para eliminar las opciones del mensaje para evitar loops
        bot.execute(new AnswerCallbackQuery(callbackQuery.id()));
        bot.execute(new EditMessageText(chatId, messageId, "You selected " + selectedRate + "% for " + companyName).replyMarkup(new InlineKeyboardMarkup()));
    }

    private void startEmailCheck() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(() -> {
            try {
                checkForUnreadEmails();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, 0, TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void checkForUnreadEmails() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("mail.imap.host", "imap.gmail.com");
        properties.put("mail.imap.port", "993");
        properties.put("mail.imap.ssl.enable", "true");
        try {
            Session session = Session.getDefaultInstance(properties);
            Store store = session.getStore("imap");
            store.connect(USERNAME, PASSWORD);

            Folder inbox = store.getFolder("INBOX");
            inbox.open(Folder.READ_WRITE);

            Message[] messages = inbox.search(new FlagTerm(new Flags(Flags.Flag.SEEN), false));

            for (int i = 0; i < messages.length; i++) {
                Message message = messages[i];
                if (message.getSubject().contains("Transaccion Realizada con su Tarjeta BAC Panama")) {
                    String content = getTextFromMessage(message);
                    String companyName = extractCompanyName(content);
                    if (!companyName.equals("CASHBACK PANAMA")) {
                        double usdValue = extractUsdValue(content);
                        double cashbackRate = getCashbackRate(companyName);
                        if (cashbackRate < 0) {
                            requestCashbackRate(companyName, usdValue);
                        } else {
                            calculateAndSendCashback(companyName, usdValue, cashbackRate);
                        }
                    }
                    message.setFlag(Flags.Flag.SEEN, true);

                }
            }
            inbox.close(false);
            store.close();
            System.out.println("Finished Reading Emails");
        } catch (Exception e) {
            Thread.sleep(5000);
            e.printStackTrace();
        }
    }

    private double getCashbackRate(String companyName) {
        try {
            JSONArray companiesArray = loadCashbackRates();
            for (Object obj : companiesArray) {
                JSONObject companyObj = (JSONObject) obj;
                if (companyObj.get("company").equals(companyName)) {
                    return ((Number) companyObj.get("rate")).doubleValue();
                }
            }
        } catch (IOException | JSONException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return -1;
    }

    // Envia el mensaje con las opciones
    private void requestCashbackRate(String companyName, double usdValue) {
        InlineKeyboardMarkup keyboard = new InlineKeyboardMarkup(
                new InlineKeyboardButton("1%").callbackData("1"),
                new InlineKeyboardButton("2%").callbackData("2"),
                new InlineKeyboardButton("5%").callbackData("5")
        );
        bot.execute(new SendMessage(CHAT_ID, companyName + ", $" + usdValue)
                .replyMarkup(keyboard));
    }


    private void calculateAndSendCashback(String companyName, double usdValue, double cashbackRate) throws IOException, ParseException {
        double cashback = Double.parseDouble(df.format(usdValue * cashbackRate));
        saveRecord(companyName, usdValue, cashback, cashbackRate);
        calculateNewCashBack();
        bot.execute(new SendMessage(CHAT_ID, "Cashback of " + cashback + " added for " + companyName));
        bot.execute(new SendMessage(CHAT_ID, "New total: $" + df.format(newCashBackTotal) + ", old total: $" + df.format(oldCashBackTotal)));
        calculateOldCashBack();
    }

    //Guarda el nuevo cashback en el registro.json
    private void saveRecord(String name, double total, double cashback, double cashbackRate) {
        try {
            JSONParser parser = new JSONParser();
            JSONObject registros = new JSONObject();
            JSONArray companiesArray = loadCashbackRates();
            //Si ya existe cargalo
            if (Files.exists(Paths.get("registros.json"))) {
                String content = new String(Files.readAllBytes(Paths.get("registros.json")));
                registros = (JSONObject) parser.parse(content);
            }

            //Si no existe la empresa,crealo en el json
            if (!registros.containsKey(name)) {
                registros.put(name, new JSONArray());
            }
            DecimalFormat df = new DecimalFormat("#.##");
            JSONObject entry = new JSONObject();
            entry.put("total", total);
            entry.put("porcentaje", cashbackRate * 100);
            entry.put("cashback", Double.parseDouble(df.format(cashback)));

            // Obtiene el array de la empresa y lo guarda dentro
            JSONArray companyTransactions = (JSONArray) registros.get(name);
            companyTransactions.add(entry);


            try (FileWriter file = new FileWriter("registros.json")) {
                file.write(registros.toJSONString());
                file.flush();
            }
            if (getCashbackRate(name) == -1.0) {
                JSONObject newCompanyObj = new JSONObject();
                newCompanyObj.put("company", name);
                newCompanyObj.put("rate", cashbackRate);
                companiesArray.add(newCompanyObj);
                saveCashbackRates(companiesArray);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getTextFromMessage(Message message) throws Exception {
        if (message.isMimeType("text/plain")) {
            return message.getContent().toString();
        } else if (message.isMimeType("multipart/*")) {
            MimeMultipart mimeMultipart = (MimeMultipart) message.getContent();
            return getTextFromMimeMultipart(mimeMultipart);
        }
        return "";
    }

    private String getTextFromMimeMultipart(MimeMultipart mimeMultipart) throws Exception {
        StringBuilder result = new StringBuilder();
        int count = mimeMultipart.getCount();
        for (int i = 0; i < count; i++) {
            BodyPart bodyPart = mimeMultipart.getBodyPart(i);
            if (bodyPart.isMimeType("text/plain")) {
                result.append(bodyPart.getContent());
                break;
            }
        }
        return result.toString();
    }

    private String extractCompanyName(String body) {
        Pattern pattern = Pattern.compile("Monto\\s*(.*?)\\s*USD");
        Matcher matcher = pattern.matcher(body);
        matcher.find();
        return matcher.group(1);
    }

    private double extractUsdValue(String body) {
        Pattern pattern = Pattern.compile("USD\\s([\\d,]+(?:\\.\\d{1,2})?)");
        Matcher matcher = pattern.matcher(body);
        matcher.find();
        String usdValueStr = matcher.group(1).replace(",", "");
        return Double.parseDouble(df.format(Double.parseDouble(usdValueStr)));
    }
}
