import org.springframework.beans.factory.annotation.Autowired;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.SearchOptions;

public class Example {

	@Autowired
	StatefulRediSearchConnection<String, String> connection;

	public void testSearch() {
		connection.sync().search("myIndex", "Lalo Schifrin", SearchOptions.builder().build());
	}

}
