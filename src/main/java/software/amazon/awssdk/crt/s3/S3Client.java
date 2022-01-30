
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
package software.amazon.awssdk.crt.s3;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import software.amazon.awssdk.crt.CrtResource;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.http.HttpHeader;
import software.amazon.awssdk.crt.http.HttpRequestBodyStream;
import software.amazon.awssdk.crt.io.TlsContext;
import software.amazon.awssdk.crt.io.StandardRetryOptions;
import software.amazon.awssdk.crt.Log;

public class S3Client extends CrtResource {

    private final static int NO_PORT = -1;
    private final static Charset UTF8 = java.nio.charset.StandardCharsets.UTF_8;
    private final CompletableFuture<Void> shutdownComplete = new CompletableFuture<>();
    private final String region;
    private final boolean withTls;

    public S3Client(S3ClientOptions options) throws CrtRuntimeException {
        // TODO consider just init the tls context the way it always was and override solely in get meta req
        TlsContext tlsCtx = options.getTlsContext();
        withTls  = tlsCtx != null || options.getTlsEnabled();
        region = options.getRegion();
        acquireNativeHandle(s3ClientNew(this, region.getBytes(UTF8),
                options.getEndpoint() != null ? options.getEndpoint().getBytes(UTF8) : null,
                options.getClientBootstrap().getNativeHandle(), tlsCtx != null ? tlsCtx.getNativeHandle() : 0,
                withTls,
                options.getCredentialsProvider().getNativeHandle(), options.getPartSize(),
                options.getThroughputTargetGbps(), options.getMaxConnections(), options.getStandardRetryOptions(),
                options.getComputeContentMd5()));

        addReferenceTo(options.getClientBootstrap());
        addReferenceTo(options.getCredentialsProvider());
    }

    private void onShutdownComplete() {
        releaseReferences();

        this.shutdownComplete.complete(null);
    }

    public S3MetaRequest makeMetaRequest(S3MetaRequestOptions options) {

        if (options.getHttpRequest() == null) {
            Log.log(Log.LogLevel.Error, Log.LogSubject.S3Client,
                    "S3Client.makeMetaRequest has invalid options; Http Request cannot be null.");
            return null;
        }

        if (options.getResponseHandler() == null) {
            Log.log(Log.LogLevel.Error, Log.LogSubject.S3Client,
                    "S3Client.makeMetaRequest has invalid options; Response Handler cannot be null.");
            return null;
        }

        S3MetaRequest metaRequest = new S3MetaRequest();
        S3MetaRequestResponseHandlerNativeAdapter responseHandlerNativeAdapter = new S3MetaRequestResponseHandlerNativeAdapter(
                options.getResponseHandler());

        URI requestUri = options.getURI();
        int port = NO_PORT;
        boolean useTls = withTls;
        if (requestUri != null) {
            useTls = requestUri.getScheme().equalsIgnoreCase("https");
            port = requestUri.getPort();
            // create/replace host header using URI from the options
            List<HttpHeader> h = options.getHttpRequest().getHeaders().stream()
                    .filter(e -> !e.getName().equalsIgnoreCase("Host"))
                    .collect(Collectors.toList());
            h.add(new HttpHeader("Host", requestUri.getHost()));
            options.getHttpRequest().setHeaders(h);
        }
        byte[] httpRequestBytes = options.getHttpRequest().marshalForJni();
        long credentialsProviderNativeHandle = 0;
        if (options.getCredentialsProvider() != null) {
            credentialsProviderNativeHandle = options.getCredentialsProvider().getNativeHandle();
        }
        long metaRequestNativeHandle = s3ClientMakeMetaRequest(getNativeHandle(), metaRequest, region.getBytes(UTF8),
                options.getMetaRequestType().getNativeValue(), httpRequestBytes,
                options.getHttpRequest().getBodyStream(), credentialsProviderNativeHandle,
                responseHandlerNativeAdapter, useTls, port);

        metaRequest.setMetaRequestNativeHandle(metaRequestNativeHandle);
        if (credentialsProviderNativeHandle != 0) {
            /*
             * Keep the java object alive until the meta Request shut down and release all
             * the resources it's pointing to
             */
            metaRequest.addReferenceTo(options.getCredentialsProvider());
        }
        return metaRequest;
    }

    /**
     * Determines whether a resource releases its dependencies at the same time the
     * native handle is released or if it waits. Resources that wait are responsible
     * for calling releaseReferences() manually.
     */
    @Override
    protected boolean canReleaseReferencesImmediately() {
        return false;
    }

    /**
     * Cleans up the native resources associated with this client. The client is
     * unusable after this call
     */
    @Override
    protected void releaseNativeHandle() {
        if (!isNull()) {
            s3ClientDestroy(getNativeHandle());
        }
    }

    public CompletableFuture<Void> getShutdownCompleteFuture() {
        return shutdownComplete;
    }

    /*******************************************************************************
     * native methods
     ******************************************************************************/
    private static native long s3ClientNew(S3Client thisObj, byte[] region, byte[] endpoint, long clientBootstrap,
            long tlsContext, boolean useTls, long signingConfig, long partSize, double throughputTargetGbps, int maxConnections,
            StandardRetryOptions standardRetryOptions, Boolean computeContentMd5) throws CrtRuntimeException;

    private static native void s3ClientDestroy(long client);

    private static native long s3ClientMakeMetaRequest(long clientId, S3MetaRequest metaRequest, byte[] region,
            int metaRequestType, byte[] httpRequestBytes, HttpRequestBodyStream httpRequestBodyStream,
            long signingConfig, S3MetaRequestResponseHandlerNativeAdapter responseHandlerNativeAdapter,
            boolean useTls, int port);
}
