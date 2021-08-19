package poly.service.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Resource;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import poly.service.ICreateDicService;
import poly.service.comm.ICommCont;
import poly.service.comm.MongoDBComon;
import poly.service.impl.crawling.ICrawlingService;
import poly.util.CmmUtil;
import poly.util.DateUtil;

@Service("CreateDicService")
public class CreateDicService extends MongoDBComon implements ICreateDicService, ICommCont {

	// 로그 파일 생성 및 로그 출력을 위한 log4j 프레임워크의 자바 객체
	private Logger log = Logger.getLogger(this.getClass());

	@Autowired
	private MongoTemplate mongodb;

	@Resource(name = "CrawlingService")
	private ICrawlingService crawlingService;

	/**
	 * 퀴즈 데이터 사전 생성하기
	 * 
	 */
	private int doCreateNlpDic(String text) throws Exception {

		log.info(this.getClass().getName() + ".doCreateNlpDic Start!");

		int res = 0;

		String dicDir = "/quizDic/"; // 데이터 사전 폴더 위치
		String bakDir = "/quizDic/bak/"; // 백업용 데이터 사전 폴더 위치

		String dicFile = "dic_nn.txt"; // 데이터사전
		String bakFile = dicFile + "_" + DateUtil.getDateTime("yyyyMMdd");

		File orgFile = new File(dicDir + dicFile);

		// 기존 파일 삭제
		if (orgFile.exists()) {

			// 복사할 파일 생성
			File copyFile = new File(bakDir + bakFile);

			// 파일복사
			FileCopyUtils.copy(orgFile, copyFile);

			copyFile = null;

			// 기존 데이터 사전 삭제
			orgFile.delete();

		}

		BufferedWriter fw = new BufferedWriter(new FileWriter(orgFile, true));

		// 파일안에 문자열 쓰기
		fw.write(text);

		fw.flush();
		fw.close();

		orgFile = null;

		log.info(this.getClass().getName() + ".doCreateNlpDic End!");

		return res;

	}

	/**
	 * 가수이름 수집하기
	 */
	private int doColletMelon() throws Exception {

		log.info(this.getClass().getName() + ".doColletMelon Start!");

		int res = 0;

		// MongoDB에 저장할 데이터구조
		List<Document> sList = new ArrayList<Document>();

		// 데이터 수집일
		String collectDay = DateUtil.getDateTime("yyyyMMdd");

		// 멜론 가수 가져오기
		Set<String> rSet = crawlingService.doColletMelon();

		if (rSet == null) {
			rSet = new TreeSet<String>();

		}

		// 저장할 데이터 구조
		Document doc = null;

		Iterator<String> it = rSet.iterator();

		while (it.hasNext()) {
			String word = CmmUtil.nvl(it.next());

			doc = new Document();

			doc.append("word", word);
			doc.append("collect_day", collectDay);

			sList.add(doc);

			doc = null;

		}

		if (sList.size() > 0) {
			// 데이터저장하기
			MongoCollection<Document> col = mongodb.getCollection("NLP_WORD_DICTIONARY_SINGER");
			col.insertMany(sList);
			col = null;

		}

		res = 1;

		log.info(this.getClass().getName() + ".doColletMelon End!");

		return res;
	}

	/**
	 * 영화제목 수집하기
	 */
	private int doColletMovie() throws Exception {

		log.info(this.getClass().getName() + ".doColletMovie Start!");

		int res = 0;

		// MongoDB에 저장할 데이터구조
		List<Document> sList = new ArrayList<Document>();

		// 데이터 수집일
		String collectDay = DateUtil.getDateTime("yyyyMMdd");

		// 멜론 가수 가져오기
		Set<String> rSet = crawlingService.doColletMovie();

		if (rSet == null) {
			rSet = new TreeSet<String>();

		}

		// 저장할 데이터 구조
		Document doc = null;

		Iterator<String> it = rSet.iterator();

		while (it.hasNext()) {
			String word = CmmUtil.nvl(it.next());

			doc = new Document();

			doc.append("word", word);
			doc.append("collect_day", collectDay);

			sList.add(doc);

			doc = null;

		}

		if (sList.size() > 0) {
			// 데이터저장하기
			MongoCollection<Document> col = mongodb.getCollection("NLP_WORD_DICTIONARY_MOVIE");
			col.insertMany(sList);
			col = null;

		}

		res = 1;

		log.info(this.getClass().getName() + ".doColletMovie End!");

		return res;
	}

	/**
	 * 지상파 방송 수집하기
	 */
	private int doColletTV1() throws Exception {

		log.info(this.getClass().getName() + ".doColletTV1 Start!");

		int res = 0;

		// MongoDB에 저장할 데이터구조
		List<Document> sList = new ArrayList<Document>();

		// 데이터 수집일
		String collectDay = DateUtil.getDateTime("yyyyMMdd");

		// 멜론 가수 가져오기
		Set<String> rSet = crawlingService.doColletTV1();

		if (rSet == null) {
			rSet = new TreeSet<String>();

		}

		// 저장할 데이터 구조
		Document doc = null;

		Iterator<String> it = rSet.iterator();

		while (it.hasNext()) {
			String word = CmmUtil.nvl(it.next());

			doc = new Document();

			doc.append("word", word);
			doc.append("collect_day", collectDay);

			sList.add(doc);

			doc = null;

		}

		if (sList.size() > 0) {
			// 데이터저장하기
			MongoCollection<Document> col = mongodb.getCollection("NLP_WORD_DICTIONARY_TV");
			col.insertMany(sList);
			col = null;

		}

		res = 1;

		log.info(this.getClass().getName() + ".doColletTV1 End!");

		return res;
	}

	/**
	 * 종합편성 방송 수집하기
	 */
	private int doColletTV2() throws Exception {

		log.info(this.getClass().getName() + ".doColletTV2 Start!");

		int res = 0;

		// MongoDB에 저장할 데이터구조
		List<Document> sList = new ArrayList<Document>();

		// 데이터 수집일
		String collectDay = DateUtil.getDateTime("yyyyMMdd");

		// 멜론 가수 가져오기
		Set<String> rSet = crawlingService.doColletTV2();

		if (rSet == null) {
			rSet = new TreeSet<String>();

		}

		// 저장할 데이터 구조
		Document doc = null;

		Iterator<String> it = rSet.iterator();

		while (it.hasNext()) {
			String word = CmmUtil.nvl(it.next());

			doc = new Document();

			doc.append("word", word);
			doc.append("collect_day", collectDay);

			sList.add(doc);

			doc = null;

		}

		if (sList.size() > 0) {
			// 데이터저장하기
			MongoCollection<Document> col = mongodb.getCollection("NLP_WORD_DICTIONARY_TV");
			col.insertMany(sList);
			col = null;

		}

		res = 1;

		log.info(this.getClass().getName() + ".doColletTV2 End!");

		return res;
	}

	/**
	 * 케이블 방송 수집하기
	 */
	private int doColletTV3() throws Exception {

		log.info(this.getClass().getName() + ".doColletTV3 Start!");

		int res = 0;

		// MongoDB에 저장할 데이터구조
		List<Document> sList = new ArrayList<Document>();

		// 데이터 수집일
		String collectDay = DateUtil.getDateTime("yyyyMMdd");

		// 멜론 가수 가져오기
		Set<String> rSet = crawlingService.doColletTV3();

		if (rSet == null) {
			rSet = new TreeSet<String>();

		}

		// 저장할 데이터 구조
		Document doc = null;

		Iterator<String> it = rSet.iterator();

		while (it.hasNext()) {
			String word = CmmUtil.nvl(it.next());

			doc = new Document();

			doc.append("word", word);
			doc.append("collect_day", collectDay);

			sList.add(doc);

			doc = null;

		}

		if (sList.size() > 0) {
			// 데이터저장하기
			MongoCollection<Document> col = mongodb.getCollection("NLP_WORD_DICTIONARY_TV");
			col.insertMany(sList);
			col = null;

		}

		res = 1;

		log.info(this.getClass().getName() + ".doColletTV3 End!");

		return res;
	}

	// 매주 화요일 오후 2시에 데이터사전 업데이트 실행
	@Scheduled(cron = "0 0 14 * 3 *")
	@Override
	public void doCreate() throws Exception {
		log.info(this.getClass().getName() + ".doCollect Start!");

		/*
		 * 데이터 사전 만들기
		 */
		String stdDay = DateUtil.getDateTime("yyyyMMdd"); // 기준일자
		String colNm = "";

		MongoCollection<Document> col = null;
		Iterator<Document> cursor = null;
		AggregateIterable<Document> rs = null;
		FindIterable<Document> dRs = null;

		// 조회결과 저장하기
		List<Document> rList = null;

		// 쿼리
		List<? extends Bson> pipeline = null;

		/*
		 * #############################################################################
		 * 기 등록된 데이터 사전 삭제 시작!
		 * #############################################################################
		 */
		// 가수 이름 삭제
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_SINGER");
		dRs = col.find(new Document("collect_day", stdDay));
		cursor = dRs.iterator();

		while (cursor.hasNext()) {
			col.deleteOne(cursor.next());

		}

		cursor = null;
		dRs = null;
		col = null;

		// 기등록된 데이터 삭제하기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_MOVIE");
		dRs = col.find(new Document("collect_day", stdDay));
		cursor = dRs.iterator();

		while (cursor.hasNext()) {
			col.deleteOne(cursor.next());

		}

		cursor = null;
		dRs = null;
		col = null;

		// 기등록된 데이터 삭제하기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_TV");
		dRs = col.find(new Document("collect_day", stdDay));
		cursor = dRs.iterator();

		while (cursor.hasNext()) {
			col.deleteOne(cursor.next());

		}

		cursor = null;
		dRs = null;
		col = null;

		// 기등록된 데이터 삭제하기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_HISTORY");
		dRs = col.find(new Document("std_day", stdDay));
		cursor = dRs.iterator();

		while (cursor.hasNext()) {
			col.deleteOne(cursor.next());

		}

		cursor = null;
		dRs = null;
		col = null;

		/*
		 * #############################################################################
		 * 기 등록된 데이터 사전 삭제 끝!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 데이터수집 시작!
		 * #############################################################################
		 */

		// 멜론 가수이름 수집
		doColletMelon();

		// 영화 제목 수집
		doColletMovie();

		// 지상파 방송 수집
		doColletTV1();

		// 종합편성 방송 수집
		doColletTV2();

		// 케이블 방송 수집
		doColletTV3();

		/*
		 * #############################################################################
		 * 데이터수집 끝!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 데이터사전 백업 시작!
		 * #############################################################################
		 */

		Document projection = new Document();

		projection.append("word", "$word");
		projection.append("_id", 0);

		// 배포된 데이터 사전
		col = mongodb.getCollection("NLP_WORD_DICTIONARY");

		dRs = col.find(new Document()).projection(projection);
		cursor = dRs.iterator();

		// 결과 저장하기
		rList = IteratorUtils.toList(cursor);

		cursor = null;
		dRs = null;
		col = null;

		// 백업할 컬렉션에 저장
		colNm = "NLP_WORD_DICTIONARY_HISTORY";
		col = mongodb.getCollection(colNm);
		col.insertOne(new Document("std_day", stdDay).append("dic_data", rList));

		col = null;

		/*
		 * #############################################################################
		 * 데이터사전 백업 끝!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 데이터사전 생성할 데이터 가져오기 시작!
		 * #############################################################################
		 */

		pipeline = Arrays.asList(
				new Document().append("$group",
						new Document().append("_id", new Document().append("word", "$word")).append("MAX(word_length)",
								new Document().append("$max", "$word_length"))),
				new Document().append("$project", new Document().append("word", "$_id.word")
						.append("word_length", "$MAX(word_length)").append("_id", 0)));

		List<Document> tmpList = null;

		// 가수정보 중복제거하여 가져오기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_SINGER");
		rs = col.aggregate(pipeline).allowDiskUse(true);
		cursor = rs.iterator();

		// 결과 저장하기
		tmpList = IteratorUtils.toList(cursor);

		if (tmpList.size() > 0) {
			rList.addAll(tmpList);

		}

		tmpList = null;

		cursor = null;
		rs = null;
		col = null;

		// 영화제목 중복제거하여 가져오기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_MOVIE");

		rs = col.aggregate(pipeline).allowDiskUse(true);
		cursor = rs.iterator();

		// 결과 저장하기
		tmpList = IteratorUtils.toList(cursor);

		if (tmpList.size() > 0) {
			rList.addAll(tmpList);

		}

		tmpList = null;

		cursor = null;
		rs = null;
		col = null;

		// 방송제목 중복제거하여 가져오기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_TV");

		rs = col.aggregate(pipeline).allowDiskUse(true);
		cursor = rs.iterator();

		// 결과 저장하기
		tmpList = IteratorUtils.toList(cursor);

		if (tmpList.size() > 0) {
			rList.addAll(tmpList);

		}

		tmpList = null;

		cursor = null;
		rs = null;
		col = null;

		// 사용자 정의사전에서 가져오기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_USER_DEF");

		rs = col.aggregate(pipeline).allowDiskUse(true);
		cursor = rs.iterator();

		// 결과 저장하기
		tmpList = IteratorUtils.toList(cursor);

		if (tmpList.size() > 0) {
			rList.addAll(tmpList);

		}

		tmpList = null;

		cursor = null;
		rs = null;
		col = null;
		
		// 신조어 사전에서 가져오기
		col = mongodb.getCollection("NLP_WORD_DICTIONARY_NEW_WORD");
		
		rs = col.aggregate(pipeline).allowDiskUse(true);
		cursor = rs.iterator();
		
		// 결과 저장하기
		tmpList = IteratorUtils.toList(cursor);
		
		if (tmpList.size() > 0) {
			rList.addAll(tmpList);
			
		}
		
		tmpList = null;
		
		cursor = null;
		rs = null;
		col = null;



		/*
		 * #############################################################################
		 * 데이터사전 생성할 데이터 가져오기 끝!
		 * #############################################################################
		 */

		/*
		 * #############################################################################
		 * 데이터사전 만들기 시작!
		 * #############################################################################
		 */

		colNm = "NLP_WORD_DICTIONARY_TEMP_" + stdDay;

		// 컬렉션 생성
		super.DeleteCreateCollection(colNm, "word");

		col = mongodb.getCollection(colNm);
		col.insertMany(rList);

		rList = null;
		col = null;

		pipeline = null;

		// 데이터 사전 데이터 만들기
		pipeline = Arrays.asList(
				new Document().append("$group", new Document().append("_id", new Document().append("word", "$word"))),
				new Document().append("$project",
						new Document().append("word", "$_id.word")
								.append("word_length", new Document("$strLenCP", "$_id.word")).append("_id", 0)),
				new Document().append("$sort", new Document().append("word_length", -1).append("word", 1)));

		col = mongodb.getCollection(colNm);
		rs = col.aggregate(pipeline).allowDiskUse(true);
		cursor = rs.iterator();

		String text = "\n";

		List<Document> sList = new LinkedList<Document>();

		while (cursor.hasNext()) {
			Document doc = cursor.next();

			if (doc == null) {
				doc = new Document();

			}

			String word = CmmUtil.nvl(doc.getString("word"));
//			int word_length2 = doc.getInteger("word_length");
			text += (word + "\tNNP\n");

//			log.info("word : " + word + " / word_length2 : " + word_length2);
			sList.add(new Document("word", word));

		}

		cursor = null;
		rs = null;
		col = null;
		pipeline = null;

		// 데이터 사전 생성하기
		doCreateNlpDic(text);

		colNm = "NLP_WORD_DICTIONARY";

		// 컬렉션 생성
		super.DeleteCreateCollectionUniqueIndex(colNm, "word");

		col = mongodb.getCollection(colNm);
		col.insertMany(sList);
		col = null;

		sList = null;

		/*
		 * #############################################################################
		 * 데이터사전 만들기 끝!
		 * #############################################################################
		 */

		// 임시 컬렉션 삭제함
		colNm = "NLP_WORD_DICTIONARY_TEMP_" + stdDay;
		mongodb.dropCollection(colNm);

		log.info(this.getClass().getName() + ".doCollect End!");

	}

}
