package com.neo.datamanager;

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;


/**
 * 更新xml节点内容,删除节点,更新节点
 */
public class Modify_File {

	/**
	 * 获取文件
	 * @param dir
	 * @param v_src_dir
	 * @param v_tag_dir
	 */
	public void createFile(File dir,String v_src_dir,String v_tag_dir,String [] v_keys){
		File[] files = dir.listFiles();
		for (int i = 0; i < files.length; i++) {
			File f = files[i];
			if (f.isFile()){
				if (f.getName().contains(".kjb")){
					writeFile(f,v_src_dir,v_tag_dir,v_keys);
				}
			}
			if(f.isDirectory()){
				createFile(f,v_src_dir,v_tag_dir,v_keys);
			}
		}
	}

	/**
	 * 删除目录以及目录下的所有文件以及文件夹
	 * @param v_dir
	 * @return
	 */
	public Boolean deleteDir(String v_dir){
		Boolean flag = false;
		File td = new File(v_dir);
		if(!td.exists()){
			return false;
		}
		if(!td.isDirectory()){
			return flag;
		}
		File[] files = td.listFiles();
		for (int i = 0; i < files.length; i++) {
			File f = files[i];
			if (f.isFile()){
				f.delete();
			}
			if (f.isDirectory()){
				deleteDir(f.toString());
			}
		}
		td.delete();
		flag = true;
		return flag;
	}

	/**
	 * 生成新文件
	 * @param file
	 * @param v_src_dir
	 * @param v_tag_dir
	 * @param v_keys
	 */
	public void writeFile(File file,String v_src_dir,String v_tag_dir,String [] v_keys){
		try {
			InputStream is = new FileInputStream(file);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String filename = file.getName();
			File tmp_file = new File(file.getParentFile().getAbsolutePath().replace(v_src_dir,v_tag_dir) + "\\" + filename.split("\\.")[0] + ".kjb");
			if(!tmp_file.getParentFile().exists()){
				boolean result = tmp_file.getParentFile().mkdirs();
				if (!result){
					System.out.println("目录已经存在!!");
				}
			}
			if (tmp_file.exists()){
				tmp_file.delete();
			}
			BufferedWriter bw = new BufferedWriter(new FileWriter(tmp_file,true));
			String old_content = null;
			while (true){
				old_content = br.readLine();
				if (old_content == null){
					break;
				}
				bw.write(old_content + "\n");
			}
			is.close();
			br.close();
			bw.close();
			repaceXML(tmp_file,v_keys);
			System.out.println("文件生成成功:" + tmp_file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 1、删除父节点connection以及所有子节点
	 * 2、把父节点job-log-table以及jobentry-log-table下的三个子节点的值更新为空
	 * @param v_file
	 * @param v_keys
	 */
	public void repaceXML(File v_file,String [] v_keys){
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		try {
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = 	db.parse(v_file);
			for (int k = 0; k < v_keys.length; k++) {
				NodeList nl = doc.getElementsByTagName(v_keys[k]);
				for (int i = 0; i < nl.getLength(); i++) {
					Node conn_node = nl.item(i);
//					//获取当前节点的属性
//					NamedNodeMap attrs = conn_node.getAttributes();
//					//遍历属性信息
//					for (int j = 0; j < attrs.getLength(); j++) {
//						Node attr = attrs.item(j);
//						System.out.println(attr.getNodeName() + ":" + attr.getNodeValue());
//					}
					//遍历子节点
					NodeList childNods = conn_node.getChildNodes();
					for (int j = 0; j < childNods.getLength(); j++) {
						Node child = childNods.item(j);
						String nodename = child.getNodeName();
						String nodevaule = child.getTextContent();
						if(v_keys[k] == "connection") {
							if (nodename == "username" && nodevaule.contains("XN_CM_LOG_DB_USER")) {
								conn_node.getParentNode().removeChild(conn_node);
								System.out.println("节点:" + v_keys[k] + " 删除成功!!");
								break;
							}
						}else{
							if(nodename == "connection"||nodename == "schema"||nodename == "table"){
								child.setTextContent("");
								System.out.println("父节点:" + v_keys[k] + "下的子节点:" + nodename + "的值更新为空!");
							}
						}
					}
				}
			}
			TransformerFactory transFactory = TransformerFactory.newInstance();
			Transformer transformer = transFactory.newTransformer();
			DOMSource domSource = new DOMSource();
			domSource.setNode(doc);
			StreamResult result = new StreamResult(v_file);
			transformer.transform(domSource, result);
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (TransformerConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		String src_dir = "D:\\nbd\\002_scheduler\\02_kettle\\bi";
		String tag_dir = src_dir + "_new";
		Modify_File mf = new Modify_File();
		File file = new File(src_dir);
		mf.deleteDir(tag_dir);
		System.out.println("目录:" + tag_dir + "删除完成!!!");
		String [] keys = {"connection","job-log-table","jobentry-log-table"};
		mf.createFile(file,src_dir,tag_dir,keys);
	}
}