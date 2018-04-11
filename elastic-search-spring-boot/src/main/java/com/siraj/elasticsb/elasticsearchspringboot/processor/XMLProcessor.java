package com.siraj.elasticsb.elasticsearchspringboot.processor;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.ArrayList;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class XMLProcessor {
    static String jsonPrettyPrintString;
    static JSONObject xmlJSONObj;

    private static final String INPUT_ZIP_FILE = System.getProperty("user.dir")+System.getProperty("file.separator")+"xml.zip";
    private static final String OUTPUT_FOLDER = System.getProperty("user.dir")+System.getProperty("file.separator");
    private static final String OUTPUT_FILE = System.getProperty("user.dir")+System.getProperty("file.separator")+"xml.xml";


    public ArrayList<JSONObject> processFile(){

        ArrayList<JSONObject> jsonObjects = new ArrayList<>();

        try {

            XMLProcessor unZip = new XMLProcessor();
            unZip.unZipIt(INPUT_ZIP_FILE,OUTPUT_FOLDER);

            String XMLString = convertXMLDocumentToString(OUTPUT_FILE);

            xmlJSONObj = XML.toJSONObject(XMLString);
            jsonPrettyPrintString = xmlJSONObj.toString();
            System.out.println(jsonPrettyPrintString);

        } catch (Exception je) {
            System.out.println("Exception: "+je.toString());
            return jsonObjects;
        }

        JSONArray jsonArray;
        try {
            jsonArray = (JSONArray) xmlJSONObj.getJSONObject("import").getJSONObject("products").get("product");
        } catch (JSONException e) {
            System.out.println(e.toString());
            return jsonObjects;
        }

        System.out.println(jsonArray.length());
        for (int i = 0; i < jsonArray.length(); i++) {
            try {
                JSONArray ja = new JSONArray();
                System.out.println("Count: "+i+": "+jsonArray.get(i));
            } catch (JSONException e) {
                System.out.println(e.toString());
                return jsonObjects;
            }
            JSONObject jo = new JSONObject();
            // populate the array
            try {

                jo.put("product",jsonArray.get(i));
                jsonObjects.add(jo.put("product",jsonArray.get(i)));
            } catch (JSONException e) {
                System.out.println(e.toString());
                return jsonObjects;
            }
            System.out.println("NewJSONObject: "+jo.toString());
        }

        return jsonObjects;
    }

    public void unZipIt(String zipFile, String outputFolder){

        byte[] buffer = new byte[1024];

        try{

            //create output directory is not exists
            File folder = new File(OUTPUT_FOLDER);
            if(!folder.exists()){
                folder.mkdir();
            }

            //get the zip file content
            ZipInputStream zis =
                    new ZipInputStream(new FileInputStream(zipFile));
            //get the zipped file list entry
            ZipEntry ze = zis.getNextEntry();

            while(ze!=null){

                String fileName = ze.getName();
                File newFile = new File(outputFolder + File.separator + fileName);

                System.out.println("file unzip : "+ newFile.getAbsoluteFile());

                //create all non exists folders
                //else you will hit FileNotFoundException for compressed folder
                new File(newFile.getParent()).mkdirs();

                FileOutputStream fos = new FileOutputStream(newFile);

                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }

                fos.close();
                ze = zis.getNextEntry();
            }

            zis.closeEntry();
            zis.close();

            System.out.println("Done");

        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

    private static String convertXMLDocumentToString(String outputFile) {
        try {
            File fXmlFile = new File(outputFile);
            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            InputStream iStream = new FileInputStream(fXmlFile);
            Document document = documentBuilderFactory.newDocumentBuilder().parse(iStream);
            StringWriter stringWriter = new StringWriter();
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "false");
            transformer.transform(new DOMSource(document), new StreamResult(stringWriter));
            String output = stringWriter.toString();
//            System.out.println(output.replaceAll("\n|\r|\\s+", ""));
            return output;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }

}
