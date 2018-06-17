package com.siraj.elasticsb.elasticsearchspringboot.resource;

import com.siraj.elasticsb.elasticsearchspringboot.processor.XMLProcessor;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@RestController
@RequestMapping("rest/products")
public class ProductsResource {

    TransportClient transportClient;

    public ProductsResource() throws UnknownHostException {
        transportClient = new PreBuiltTransportClient(Settings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    @GetMapping("/insert/{id}")
    public String get(@PathVariable final String id) throws IOException {

        IndexResponse response = transportClient.prepareIndex("product", "id", id)
                .setSource(jsonBuilder()
                        .startObject()
                        .field("name", "Siraj")
                        .field("salary", 1200)
                        .field("teamName", "Development")
                        .endObject()
                )
                .get();
        return response.getResult().toString();
    }

    @GetMapping("/view/{id}")
    public Object view(@PathVariable final String id){
        GetResponse getResponse = transportClient.prepareGet("product", "id", id).get();
        System.out.println(getResponse.getSource());

        return getResponse.getSource();
    }

    @GetMapping("/update/{id}")
    public String update(@PathVariable final String id) throws IOException {

        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("product")
                .type("id")
                .id(id)
                .doc(jsonBuilder()
                        .startObject()
                        .field("gender", "male")
                        .endObject());
        try {
            UpdateResponse updateResponse = transportClient.update(updateRequest).get();
            System.out.println(updateResponse.status());
            return updateResponse.status().toString();
        } catch (InterruptedException | ExecutionException e) {
            System.out.println(e);
        }
        return "Exception";
    }

    @GetMapping("/delete/{id}")
    public String delete(@PathVariable final String id) {

        DeleteResponse deleteResponse = transportClient.prepareDelete("product", "id", id).get();

        System.out.println(deleteResponse.getResult().toString());
        return deleteResponse.getResult().toString();
    }

    @GetMapping("/insertXML")
    public int insertxml() throws IOException {

        XMLProcessor xmlProcessor = new XMLProcessor();
        ArrayList<JSONObject> jsonObjects = xmlProcessor.processFile();

        IndexResponse response;
        int j=1;
        for (int i = 0; i < jsonObjects.size(); i++) {
            System.out.println("UsersResource: "+jsonObjects.get(i));
            response = transportClient.prepareIndex("product", "id", String.valueOf(j))
                    .setSource(jsonObjects.get(i).toString())
                    .get();

            j=j+1;
            // Index name
            String _index = response.getIndex();
            // Type name
            String _type = response.getType();
            // Document ID (generated or not)
            String _id = response.getId();
            // Version (if it's the first time you index this document, you will get: 1)
            long _version = response.getVersion();
            // isCreated() is true if the document is a new one, false if it has been updated
            int created = response.status().getStatus();
            System.out.println("Index Name: "+_index+": Type name: "+ _type+": Document ID: "+_id
                    +": Version: "+_version+": isCreated: "+created+": "+response.getResult().toString()
                    +": RestStatus: "+response.status());

        }

//       return response.getResult().toString();
        return jsonObjects.size();
    }

    @GetMapping("/bulk")
    public String bulkUpsert(){

        XMLProcessor xmlProcessor = new XMLProcessor();
        ArrayList<JSONObject> jsonObjects = xmlProcessor.processFile();

        try {
            BulkRequestBuilder bulkRequest = transportClient.prepareBulk();
            int j=1;
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < jsonObjects.size(); i++) {
                String id = UUIDs.base64UUID();
//                String lUUID = String.format("%040d", new BigInteger(UUID.randomUUID().toString().replace("-", ""), 8));
                bulkRequest.add(
                        transportClient.prepareIndex("product", "id", id)
                                .setSource(jsonObjects.get(i)
                                        .toString(),XContentType.JSON));
                j=j+1;
                stringBuilder.append(", ").append(id);
                System.out.println("ID: "+id);
            }
            BulkResponse result = bulkRequest.execute().get();
            System.out.println("Has Failures? "+result.hasFailures());
            return (result.hasFailures())?"Some failures during execution":"Successfully executed with ids "+stringBuilder.toString();
        }catch (Exception e) {
            System.out.println("Exception: "+e.getMessage());
        }
        return "Error occured";
    }
}
