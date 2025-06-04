package org.raft.client;

import org.raft.kvstore.rpc.ClientRequest;
import org.raft.kvstore.rpc.ClientResponse;
import org.raft.kvstore.rpc.KVStoreServiceGrpc;
import org.raft.kvstore.rpc.RequestLogArgs;
import org.raft.kvstore.rpc.RequestLogReply;
import org.raft.raft.rpc.LogEntry; // Impor LogEntry jika ingin menampilkannya dengan baik
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client {
//    private static final Logger logger = Logger.getLogger(Client.class.getName());
//    private ManagedChannel channel;
//    private KVStoreServiceGrpc.KVStoreServiceBlockingStub blockingStub;
//    private String currentServerAddress;
//    private static final int MAX_REDIRECTS = 5; // Maksimum jumlah redirect untuk menghindari loop tak terbatas
//
//    public Client(String initialServerAddress) {
//        this.currentServerAddress = initialServerAddress;
//        connect(initialServerAddress);
//    }
//
//    private void connect(String serverAddress) {
//        shutdown(); // Tutup channel yang ada jika ada
//        logger.info("Mencoba terhubung ke server: " + serverAddress);
//        try {
//            this.channel = ManagedChannelBuilder.forTarget(serverAddress)
//                    .usePlaintext() // Hanya untuk development, gunakan TLS di produksi
//                    .build();
//            this.blockingStub = KVStoreServiceGrpc.newBlockingStub(channel);
//            this.currentServerAddress = serverAddress;
//            logger.info("Berhasil terhubung ke: " + serverAddress);
//        } catch (Exception e) {
//            logger.log(Level.SEVERE, "Gagal terhubung ke " + serverAddress + ": " + e.getMessage());
//            this.channel = null; // Pastikan channel null jika koneksi gagal
//            this.blockingStub = null;
//        }
//    }
//
//    public void shutdown() {
//        if (channel != null && !channel.isShutdown()) {
//            try {
//                logger.info("Menutup koneksi ke: " + currentServerAddress);
//                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                logger.log(Level.WARNING, "Gagal menutup channel dengan benar.", e);
//                Thread.currentThread().interrupt();
//            }
//        }
//        channel = null;
//        blockingStub = null;
//    }
//
//    public void executeCommand(String commandString) {
//        if (commandString == null || commandString.trim().isEmpty()) {
//            logger.warning("Perintah tidak boleh kosong.");
//            return;
//        }
//
//        // Perintah khusus untuk klien
//        if (commandString.startsWith("connect")) {
//            String[] parts = commandString.split(" ", 2);
//            if (parts.length == 2 && !parts[1].trim().isEmpty()) {
//                connect(parts[1].trim());
//            } else {
//                logger.warning("Penggunaan: connect <host:port>");
//            }
//            return;
//        }
//        if (commandString.equalsIgnoreCase("exit") || commandString.equalsIgnoreCase("quit")) {
//            System.out.println("Keluar dari klien.");
//            shutdown();
//            System.exit(0);
//        }
//
//
//        int redirects = 0;
//        String originalCommand = commandString; // Simpan perintah asli untuk retry
//
//        while (redirects < MAX_REDIRECTS) {
//            if (blockingStub == null) {
//                logger.severe("Tidak terhubung ke server manapun. Coba 'connect <host:port>' terlebih dahulu.");
//                System.out.println("ERROR: Tidak terhubung ke server.");
//                return;
//            }
//
//            try {
//                if (originalCommand.equalsIgnoreCase("REQUEST_LOG")) {
//                    RequestLogArgs logRequest = RequestLogArgs.newBuilder().build();
//                    logger.info("Mengirim REQUEST_LOG ke " + currentServerAddress);
//                    RequestLogReply logReply = blockingStub.requestLog(logRequest);
//
//                    if (logReply.getSuccess()) {
//                        System.out.println("Log dari Leader (" + logReply.getLeaderId() + "):");
//                        if (logReply.getLogsCount() == 0) {
//                            System.out.println("(Log kosong)");
//                        } else {
//                            for (int i = 0; i < logReply.getLogsCount(); i++) {
//                                LogEntry entry = logReply.getLogs(i);
//                                System.out.println("  Index " + i + ": Term=" + entry.getTerm() + ", Command='" + entry.getCommand() + "'");
//                            }
//                        }
//                        return; // Selesai
//                    } else {
//                        if (logReply.hasLeaderId() && !logReply.getLeaderId().isEmpty()) {
//                            logger.info("Bukan leader. Leader hint: " + logReply.getLeaderId() + ". Mencoba redirect...");
//                            System.out.println("REDIRECT: Bukan leader. Mencoba redirect ke " + logReply.getLeaderId());
//                            connect(logReply.getLeaderId()); // Hubungkan ke leader yang disarankan
//                            redirects++;
//                            continue; // Coba lagi perintah dengan koneksi baru
//                        } else {
//                            logger.warning("REQUEST_LOG gagal dan tidak ada hint leader.");
//                            System.out.println("ERROR: Gagal mendapatkan log, tidak ada informasi leader.");
//                            return;
//                        }
//                    }
//                } else {
//                    // Untuk perintah KV lainnya
//                    ClientRequest request = ClientRequest.newBuilder().setCommand(originalCommand).build();
//                    logger.info("Mengirim perintah '" + originalCommand + "' ke " + currentServerAddress);
//                    ClientResponse response = blockingStub.executeCommand(request);
//
//                    if (response.getSuccess()) {
//                        System.out.println(response.getResult());
//                        return; // Perintah berhasil dieksekusi
//                    } else {
//                        if (response.hasLeaderHint() && !response.getLeaderHint().isEmpty()) {
//                            logger.info("Bukan leader. Leader hint: " + response.getLeaderHint() + ". Mencoba redirect...");
//                            System.out.println("REDIRECT: Bukan leader. Mencoba redirect ke " + response.getLeaderHint());
//                            connect(response.getLeaderHint());
//                            redirects++;
//                            // Tidak perlu continue, loop akan mengulang dengan stub baru
//                        } else {
//                            // Gagal dan tidak ada hint leader, atau error lain dari leader
//                            logger.warning("Perintah gagal: " + response.getResult() + " (Tidak ada hint leader atau error dari leader)");
//                            System.out.println("ERROR: " + (response.hasResult() && !response.getResult().isEmpty() ? response.getResult() : "Perintah gagal tanpa pesan spesifik."));
//                            return;
//                        }
//                    }
//                }
//            } catch (StatusRuntimeException e) {
//                logger.log(Level.SEVERE, "RPC gagal ke " + currentServerAddress + ": " + e.getStatus(), e);
//                System.out.println("ERROR: Gagal berkomunikasi dengan server " + currentServerAddress + " (" + e.getStatus().getCode() + "). Coba server lain atau periksa koneksi.");
//                // Jika server tidak tersedia, kita mungkin ingin mencoba server lain dari daftar yang diketahui
//                // Untuk saat ini, kita hanya akan gagal.
//                return;
//            } catch (Exception e) {
//                logger.log(Level.SEVERE, "Error saat menjalankan perintah ke " + currentServerAddress + ": " + e.getMessage(), e);
//                System.out.println("ERROR: Terjadi kesalahan: " + e.getMessage());
//                return;
//            }
//        }
//
//        if (redirects >= MAX_REDIRECTS) {
//            logger.warning("Mencapai maksimum redirect. Gagal menemukan leader.");
//            System.out.println("ERROR: Gagal menemukan leader setelah " + MAX_REDIRECTS + " kali redirect.");
//        }
//    }
//
//    public static void main(String[] args) {
//        if (args.length < 1) {
//            System.err.println("Penggunaan: Client <initial_server_address:port> [perintah...]");
//            System.err.println("Jika tidak ada perintah, klien akan masuk ke mode interaktif.");
//            System.exit(1);
//        }
//
//        String initialServerAddress = args[0];
//        Client client = new Client(initialServerAddress);
//
//        if (args.length > 1) {
//            // Mode perintah tunggal
//            List<String> commandParts = Arrays.asList(args).subList(1, args.length);
//            String commandString = String.join(" ", commandParts);
//            client.executeCommand(commandString);
//            client.shutdown();
//        } else {
//            // Mode interaktif
//            System.out.println("Klien KV Store Raft Interaktif. Ketik 'exit' atau 'quit' untuk keluar.");
//            System.out.println("Ketik 'connect <host:port>' untuk mengubah server.");
//            Scanner scanner = new Scanner(System.in);
//            while (true) {
//                if (client.currentServerAddress != null) {
//                    System.out.print("[" + client.currentServerAddress + "] > ");
//                } else {
//                    System.out.print("[disconnected] > ");
//                }
//                String line = scanner.nextLine();
//                if (line == null) break; // EOF
//
//                String trimmedLine = line.trim();
//                if (trimmedLine.isEmpty()) continue;
//
//                client.executeCommand(trimmedLine); // executeCommand akan menangani 'exit' dan 'connect'
//            }
//            scanner.close();
//            client.shutdown();
//        }
//    }
}
