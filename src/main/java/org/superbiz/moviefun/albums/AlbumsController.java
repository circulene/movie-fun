package org.superbiz.moviefun.albums;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.util.IOUtils;
import org.apache.tika.Tika;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.superbiz.moviefun.blobstore.Blob;
import org.superbiz.moviefun.blobstore.BlobStore;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static java.lang.ClassLoader.getSystemResource;
import static java.lang.String.format;
import static java.nio.file.Files.readAllBytes;

@Controller
@RequestMapping("/albums")
public class AlbumsController {

    private final AlbumsBean albumsBean;
    private final BlobStore blobStore;

    public AlbumsController(AlbumsBean albumsBean, BlobStore blobStore) {
        this.albumsBean = albumsBean;
        this.blobStore = blobStore;
    }

    @GetMapping
    public String index(Map<String, Object> model) {
        model.put("albums", albumsBean.getAlbums());
        return "albums";
    }

    @GetMapping("/{albumId}")
    public String details(@PathVariable long albumId, Map<String, Object> model) {
        model.put("album", albumsBean.find(albumId));
        return "albumDetails";
    }

    @PostMapping("/{albumId}/cover")
    public String uploadCover(@PathVariable long albumId, @RequestParam("file") MultipartFile uploadedFile) throws IOException {
//        saveUploadToFile(uploadedFile, getCoverFile(albumId));
        Blob blob = new Blob(getCoverNameByAlbumId(albumId), uploadedFile.getInputStream(), uploadedFile.getContentType());
        blobStore.put(blob);
        return format("redirect:/albums/%d", albumId);
    }

    @GetMapping("/{albumId}/cover")
    public HttpEntity<byte[]> getCover(@PathVariable long albumId) throws IOException, URISyntaxException {
//        Path coverFilePath = getExistingCoverPath(albumId);
//        byte[] imageBytes = readAllBytes(coverFilePath);
//        HttpHeaders headers = createImageHttpHeaders(coverFilePath, imageBytes);

        Blob blob = blobStore.get(getCoverNameByAlbumId(albumId)).orElseGet(() -> {
            InputStream stream = ClassLoader.getSystemResourceAsStream("default-cover.jpg");
            return new Blob("default-cover", stream, MediaType.IMAGE_JPEG_VALUE);
        });
        byte[] imageBytes = IOUtils.toByteArray(blob.inputStream);
        HttpHeaders headers = createImageHttpHeaders(blob, imageBytes);

        return new HttpEntity<>(imageBytes, headers);
    }

    private String getCoverNameByAlbumId(long albumId) {
        return "covers/" + albumId;
    }

//    private void saveUploadToFile(@RequestParam("file") MultipartFile uploadedFile, File targetFile) throws IOException {
//        targetFile.delete();
//        targetFile.getParentFile().mkdirs();
//        targetFile.createNewFile();
//
//        try (FileOutputStream outputStream = new FileOutputStream(targetFile)) {
//            outputStream.write(uploadedFile.getBytes());
//        }
//    }

//    private HttpHeaders createImageHttpHeaders(Path coverFilePath, byte[] imageBytes) throws IOException {
//        String contentType = new Tika().detect(coverFilePath);
//
//        HttpHeaders headers = new HttpHeaders();
//        headers.setContentType(MediaType.parseMediaType(contentType));
//        headers.setContentLength(imageBytes.length);
//        return headers;
//    }

    private HttpHeaders createImageHttpHeaders(Blob blob, byte[] imageBytes) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType(blob.contentType));
        headers.setContentLength(imageBytes.length);
        return headers;
    }

//    private File getCoverFile(@PathVariable long albumId) {
//        String coverFileName = format("covers/%d", albumId);
//        return new File(coverFileName);
//    }

//    private Path getExistingCoverPath(@PathVariable long albumId) throws URISyntaxException {
//        File coverFile = getCoverFile(albumId);
//        Path coverFilePath;
//
//        if (coverFile.exists()) {
//            coverFilePath = coverFile.toPath();
//        } else {
//            coverFilePath = Paths.get(getSystemResource("default-cover.jpg").toURI());
//        }
//
//        return coverFilePath;
//    }

}
