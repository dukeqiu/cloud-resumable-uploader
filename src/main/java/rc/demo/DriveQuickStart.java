package rc.demo;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.FileList;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DriveQuickStart {

    private static final String APPLICATION_NAME = "TestUpload";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final String TOKENS_DIRECTORY_PATH = "tokens";

    public static final int CHUNK_LIMIT = 262144;

    public static final int OK          = 200;
    public static final int CREATED     = 201;
    public static final int INCOMPLETE  = 308;

    private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);
    private static final String CREDENTIALS_FILE_PATH = "/credentials.json";
    private static final String TARGET_FOLDER = "JoeTest";
    private static NetHttpTransport HTTP_TRANSPORT;

    private final String FILE_UPLOAD = "https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable";
    private static final String FILE_PATH = "/Users/joe.zhang/Downloads/swt-4.7.1a-cocoa-macosx-x86_64.zip";

    static {
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        DriveQuickStart app = new DriveQuickStart();
        try{
            File file = new File(FILE_PATH);
            app.uploadFile(file);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
        }
    }

    public void uploadFile(File file) throws Exception
    {
        long fileSize = file.length();
        Credential credential = getCredentials(HTTP_TRANSPORT);
        String sessionUrl = initSessionUri(credential, file, getTargetFolderId());
        System.out.println(sessionUrl);
        if (StringUtils.isBlank(sessionUrl)){
            return;
        }

        for(long i = 1, j = CHUNK_LIMIT; i <= fileSize; i+=CHUNK_LIMIT)
        {
            if(i+CHUNK_LIMIT >= fileSize)
            {
                j = fileSize - i + 1;
            }
            int responseCode = uploadFilePacket(sessionUrl, file, i-1, j, fileSize, credential);
            if(!(responseCode == OK || responseCode == CREATED || responseCode == INCOMPLETE)){
                throw new RuntimeException(""+responseCode);
            }
        }
    }

    private Credential getCredentials(NetHttpTransport HTTP_TRANSPORT) throws IOException {
        // Load client secrets.
        InputStream in = DriveQuickStart.class.getResourceAsStream(CREDENTIALS_FILE_PATH);
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                .setDataStoreFactory(new FileDataStoreFactory(new File(TOKENS_DIRECTORY_PATH)))
                .setAccessType("offline")
                .build();
        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8889).build();
        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
    }

    private String getTargetFolderId() throws IOException{

        Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
                .setApplicationName(APPLICATION_NAME)
                .build();

        FileList result = service.files().list()
                .setPageSize(100)
                .setFields("nextPageToken, files(id, name)")
                .execute();
        List<com.google.api.services.drive.model.File> files = result.getFiles();
        if (files == null || files.isEmpty()) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
            for (com.google.api.services.drive.model.File file : files) {
                if(file.getName().equalsIgnoreCase(TARGET_FOLDER)){
                    return file.getId();
                }
            }
        }
        throw new FileNotFoundException(TARGET_FOLDER);
    }

    private String initSessionUri(Credential csc, File file, String parentId) throws Exception{
        GenericUrl genericUrl = new GenericUrl(FILE_UPLOAD);
        GenericData data = new GenericData();

        Map<String, Object> params = new HashMap<>();
        params.put("name", file.getName());
        params.put("parents", Collections.singletonList(parentId));
        for(Map.Entry<String, Object> entry : params.entrySet()){
            data.put(entry.getKey(), entry.getValue());
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentLength(file.length());
        headers.setContentType("application/json; charset=UTF-8");
        headers.set("X-Upload-Content-Type", "application/octet-stream");
        JsonHttpContent content = new JsonHttpContent(JSON_FACTORY, data);

        HttpRequestFactory requestFactory = HTTP_TRANSPORT
                .createRequestFactory((HttpRequest httpRequest) -> {
                    if (headers != null && headers.size() > 0) {
                        httpRequest.setHeaders(headers);
                    }
                    httpRequest.getHeaders().setAuthorization("Bearer " + csc.getAccessToken());
                });
        HttpRequest request = requestFactory.buildPostRequest(genericUrl, content);
        HttpResponse response = request.execute();

        return response.getHeaders().getLocation();
    }

    private int uploadFilePacket(String sessionUri, File file, long chunkStart, long uploadBytes, long fileSize,
                                 Credential credential) throws Exception{
        URL sessionUrl = new URL(sessionUri);
        HttpURLConnection uploadReq = (HttpURLConnection) sessionUrl.openConnection();

        uploadReq.setRequestMethod("PUT");
        uploadReq.setDoOutput(true);
        uploadReq.setDoInput(true);
        uploadReq.setConnectTimeout(10000);

        //uploadReq.setRequestProperty("Content-Type", "application/octet-stream");
        uploadReq.setRequestProperty("Content-Length", String.valueOf(uploadBytes));
        uploadReq.setRequestProperty("Content-Range", "bytes " + chunkStart + "-" + (chunkStart + uploadBytes -1) + "/" + fileSize);

        OutputStream outstream = uploadReq.getOutputStream();

        byte[] buffer = new byte[(int) uploadBytes];
        FileInputStream fileInputStream = new FileInputStream(file);
        fileInputStream.getChannel().position(chunkStart);
        if (fileInputStream.read(buffer, 0, (int) uploadBytes) == -1);
        fileInputStream.close();

        outstream.write(buffer);
        outstream.close();

        uploadReq.connect();
        System.out.println(credential.getAccessToken());

        return uploadReq.getResponseCode();
    }

}