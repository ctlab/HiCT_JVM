package ru.itmo.ctlab.hict.hict_server.dto.request.sequence;

import io.vertx.core.json.JsonObject;
import ru.itmo.ctlab.hict.hict_library.assembly.SequenceSearchQueries;
import ru.itmo.ctlab.hict.hict_server.handlers.conversion.RemoteBlastOptions;
import java.util.List;

/** A separate download-only route prevents older servers from treating recovery as a new submission. */
public record RemoteBlastRetrievalRequestDTO(JsonObject settings, List<SequenceSearchQueries.Query> queries) {
  public static RemoteBlastRetrievalRequestDTO fromJson(JsonObject input) {
    if (input == null || !"NCBI_REMOTE".equals(input.getValue("searchTarget"))
        || !(input.getValue("queries") instanceof String queryText))
      throw new IllegalArgumentException("Choose NCBI remote retrieval and paste the original queries");
    var queries = SequenceSearchQueries.parse(queryText);
    var options = RemoteBlastOptions.fromJson(input, queries);
    if (options.requestId() == null) throw new IllegalArgumentException("Enter the existing NCBI request ID (RID); retrieval never submits a new search");
    return new RemoteBlastRetrievalRequestDTO(input.copy().put("remoteRequestId", options.requestId()), queries);
  }
}
