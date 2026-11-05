import io.github.alex59.filestreams.StreamProfile;
import io.github.alex59.filestreams.UploadRequest;
import io.github.alex59.filestreams.s3.S3Clients;
import io.github.alex59.filestreams.s3.S3ObjectStreams;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;

// EN: Plain Java configuration uses the same engine as Boot, without null endpoint or byte arithmetic.
// RU: Чистая Java использует тот же engine, что Boot, без null endpoint и арифметики байтов.
public final class NamedProfiles {
    public static void run() {
        // EN: The application owns these resources for its full lifetime, beyond all subscribed transfers.
        // RU: Приложение владеет ресурсами весь свой срок жизни, дольше всех подписанных передач.
        try (var credentials = DefaultCredentialsProvider.builder().build();
             var client = S3Clients.bounded(Region.EU_WEST_1, credentials)) {
            var objects = new S3ObjectStreams(client, "application-attachments", StreamProfile.ATTACHMENTS);
            var request = UploadRequest.knownLength("alice:invoice.pdf", 123, "application/pdf");
            // EN: This compile/runtime consumer makes no storage request and requires no actual credentials.
            // RU: Эта проверка компиляции/runtime не обращается к storage и не требует настоящих credentials.
            if (request.length().orElseThrow() != 123) throw new AssertionError("Missing known size");
            if (objects.download(request.key()) == null) throw new AssertionError("Missing lazy download");
        }
    }
}
