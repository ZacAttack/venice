package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY;
import static com.linkedin.venice.utils.TestUtils.getVenicePathParser;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.alpini.netty4.misc.BasicFullHttpRequest;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.CompressorFactory;
import com.linkedin.venice.meta.NameRepository;
import com.linkedin.venice.meta.StoreVersionName;
import com.linkedin.venice.router.stats.AggRouterHttpRequestStats;
import com.linkedin.venice.router.stats.RouterStats;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestVeniceResponseDecompressor {
  private final NameRepository nameRepository = new NameRepository();

  /**
   * If client supports decompression and the single get request was successful, then the router should return the
   * compression strategy in the response header.
   */
  @Test
  public void testRouterPassThroughContentIfClientSupportsDecompression() {
    StoreVersionName storeVersionName = nameRepository.getStoreVersionName("test-store", 1);
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64",
        System.currentTimeMillis(),
        100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    CompressorFactory compressorFactory = mock(CompressorFactory.class);
    VenicePathParser pathParser = getVenicePathParser(compressorFactory, true);
    VeniceResponseDecompressor responseDecompressor = pathParser.getDecompressor(storeVersionName, request);

    CompositeByteBuf content = Unpooled.compositeBuffer();

    ContentDecompressResult result = responseDecompressor.decompressSingleGetContent(CompressionStrategy.GZIP, content);

    Assert.assertSame(result.getContent(), content);
    Assert.assertEquals(result.getCompressionStrategy(), CompressionStrategy.GZIP);
  }

  /**
   * If client doesn't support decompression and the single get request was successful, then the router should return
   * NO_OP compression strategy in the response header.
   */
  @Test
  public void testRouterDecompressesRecordIfClientDoesntSupportsDecompression() {
    StoreVersionName storeVersionName = nameRepository.getStoreVersionName("test-store", 1);
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64",
        System.currentTimeMillis(),
        100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    try (CompressorFactory compressorFactory = new CompressorFactory()) {
      compressorFactory.createVersionSpecificCompressorIfNotExist(
          CompressionStrategy.ZSTD_WITH_DICT,
          "test-store_v1",
          new byte[] {});

      VenicePathParser pathParser = getVenicePathParser(compressorFactory, true);
      VeniceResponseDecompressor responseDecompressor = pathParser.getDecompressor(storeVersionName, request);

      CompositeByteBuf content = Unpooled.compositeBuffer();

      ContentDecompressResult result =
          responseDecompressor.decompressSingleGetContent(CompressionStrategy.ZSTD_WITH_DICT, content);

      Assert.assertNotSame(result.getContent(), content);
      Assert.assertEquals(result.getCompressionStrategy(), CompressionStrategy.NO_OP);
    }
  }

  /**
   * If client doesn't support decompression and the multi get request was successful, then the router should return
   * NO_OP compression strategy in the response header.
   */
  @Test
  public void testRouterReturnsNoopCompressionStrategyHeaderIfClientDoesntSupportsDecompressionForMultiGet() {
    StoreVersionName storeVersionName = nameRepository.getStoreVersionName("test-store", 1);
    BasicFullHttpRequest request = new BasicFullHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        "storage/ZstdThreeStringFieldWithPrefix/ApqFzqwN?f=b64",
        System.currentTimeMillis(),
        100000);
    request.headers().add(VENICE_SUPPORTED_COMPRESSION_STRATEGY, CompressionStrategy.GZIP.getValue());

    RouterStats<AggRouterHttpRequestStats> routerStats = mock(RouterStats.class);
    AggRouterHttpRequestStats stats = mock(AggRouterHttpRequestStats.class);

    doReturn(stats).when(routerStats).getStatsByType(any());

    RouterExceptionAndTrackingUtils.setRouterStats(routerStats);

    try (CompressorFactory compressorFactory = new CompressorFactory()) {
      compressorFactory.createVersionSpecificCompressorIfNotExist(
          CompressionStrategy.ZSTD_WITH_DICT,
          "test-store_v1",
          new byte[] {});

      VenicePathParser pathParser = getVenicePathParser(compressorFactory, true);
      VeniceResponseDecompressor responseDecompressor = pathParser.getDecompressor(storeVersionName, request);

      CompositeByteBuf content1 = Unpooled.compositeBuffer();
      ContentDecompressResult result =
          responseDecompressor.decompressMultiGetContent(CompressionStrategy.ZSTD_WITH_DICT, content1);
      Assert.assertEquals(result.getCompressionStrategy(), CompressionStrategy.NO_OP);
    }
  }
}
