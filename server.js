import express from "express"; // Express: 서버 생성
import axios from "axios"; // Axios: HTTP 요청을 위한 라이브러리
import fs from "fs"; // File System: 파일 읽기/쓰기
import os from "os"; // OS: 시스템 정보
import { Kafka, Partitioners } from "kafkajs"; // Kafka.js: Kafka 통합
import https from "https"; // HTTPS: Kubernetes API 요청

// Express 서버 설정
const app = express();
const PORT = process.env.PORT || 3000; // 기본 포트 설정

// OpenAPI와 Kafka 설정
const API_KEY = process.env.API_KEY || "6d6453566477696e37326641706c54"; // OpenAPI 인증 키
const BASE_URL = "http://openapi.seoul.go.kr:8088"; // OpenAPI 기본 URL

const kafkaBrokers = process.env.KAFKA_BROKERS || "10.10.150.124:9092"; // Kafka 브로커 주소
const kafka = new Kafka({
  clientId: "parking-api-client", // Kafka 클라이언트 ID
  brokers: kafkaBrokers.split(","), // 브로커 목록
});
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner, // 기본 파티셔너 사용
});

const POD_NAME = process.env.POD_NAME || os.hostname(); // Pod 이름 가져오기
let sharedTotalData = null; // Pod 간 공유할 데이터 크기 변수

// Kubernetes Pod 세부 정보 가져오기
async function getPodDetails(namespace = "default") {
  try {
    const response = await axios.get(
      `https://kubernetes.default.svc/api/v1/namespaces/${namespace}/pods`,
      {
        headers: {
          Authorization: `Bearer ${fs.readFileSync(
            "/var/run/secrets/kubernetes.io/serviceaccount/token"
          )}`,
        },
        httpsAgent: new https.Agent({
          ca: fs.readFileSync(
            "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
          ),
        }),
      }
    );

    const pods = response.data.items
      .filter((pod) => pod.metadata.labels.app === "parking-api")
      .sort((a, b) => {
        const nameA = a.metadata.name.match(/\d+/)?.[0] || "0";
        const nameB = b.metadata.name.match(/\d+/)?.[0] || "0";
        return parseInt(nameA) - parseInt(nameB);
      });

    console.log(`Pod List: ${pods.map((pod) => pod.metadata.name)}`);
    return {
      podCount: pods.length, // 전체 Pod 개수
      podIndex: pods.findIndex((pod) => pod.metadata.name === POD_NAME), // 현재 Pod 인덱스
    };
  } catch (error) {
    console.error("Error fetching Pod details:", error.message);
    return { podCount: 1, podIndex: 0 }; // 기본값 반환
  }
}

// OpenAPI를 통해 전체 데이터 개수 가져오기
async function fetchTotalDataCount() {
  const url = `${BASE_URL}/${API_KEY}/json/GetParkingInfo/1/1`;
  try {
    const response = await axios.get(url);
    if (response.data.GetParkingInfo?.RESULT?.CODE !== "INFO-000") {
      throw new Error(`OpenAPI 오류: ${response.data.GetParkingInfo?.RESULT?.MESSAGE}`);
    }
    return response.data.GetParkingInfo?.list_total_count || 0; // 전체 데이터 개수 반환
  } catch (error) {
    console.error(`OpenAPI 데이터 크기 조회 실패: ${error.message}`);
    throw error;
  }
}

// OpenAPI를 통해 주차 데이터를 가져오기
async function fetchParkingData(startIndex, endIndex) {
  const MAX_BATCH_SIZE = 1000; // OpenAPI 요청 최대 크기
  let allData = [];

  for (let i = startIndex; i <= endIndex; i += MAX_BATCH_SIZE) {
    const batchStart = i;
    const batchEnd = Math.min(i + MAX_BATCH_SIZE - 1, endIndex);
    const url = `${BASE_URL}/${API_KEY}/json/GetParkingInfo/${batchStart}/${batchEnd}`;

    try {
      const response = await axios.get(url);

      if (response.data.GetParkingInfo?.RESULT?.CODE !== "INFO-000") {
        throw new Error(
          `OpenAPI 오류: ${response.data.GetParkingInfo?.RESULT?.MESSAGE}`
        );
      }

      allData = allData.concat(response.data.GetParkingInfo?.row || []); // 데이터 누적
    } catch (error) {
      console.error(
        `OpenAPI 호출 실패 (범위 ${batchStart} ~ ${batchEnd}): ${error.message}`
      );
      throw error;
    }
  }

  return allData; // 전체 데이터 반환
}

// Kafka로 데이터 전송
async function sendToKafka(topic, data) {
  const key = `${POD_NAME}-${data.startIndex}-${data.endIndex}-${Date.now()}`; // 고유한 키 생성
  try {
    await producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(data) }], // 데이터 전송
    });
    console.log(`Kafka 전송 성공: key=${key}, 데이터 개수=${data.data.length}`);
  } catch (error) {
    console.error(`Kafka 메시지 전송 실패: ${error.message}`);
  }
}

// Pod별 처리 범위 계산
function calculateRange(totalData, podIndex, podCount) {
  const rangeSize = Math.floor(totalData / podCount); // 각 Pod가 처리할 데이터 크기
  const startIndex = podIndex * rangeSize + 1;
  const endIndex = podIndex === podCount - 1 
    ? totalData  // 마지막 Pod는 남은 데이터를 모두 처리
    : (podIndex + 1) * rangeSize;
  console.log(`Pod ${POD_NAME} 범위: ${startIndex} ~ ${endIndex}`);
  return { startIndex, endIndex };
}

// 작업 처리 메인 함수
async function processWork() {
  try {
    const { podCount, podIndex } = await getPodDetails(); // Pod 정보 가져오기

    // `totalData`를 처음 요청에서만 저장하고, 이후엔 공유
    if (!sharedTotalData) {
      sharedTotalData = await fetchTotalDataCount();
    }

    console.log(`총 데이터 크기: ${sharedTotalData}, Pod Count: ${podCount}`);

    const { startIndex, endIndex } = calculateRange(sharedTotalData, podIndex, podCount); // 범위 계산
    console.log(`Pod ${POD_NAME}: 처리 범위 ${startIndex} ~ ${endIndex}`);

    const data = await fetchParkingData(startIndex, endIndex); // 데이터 가져오기

    // 특정 구 필터링
    const targetDistricts = ["중구", "용산구", "관악구", "서대문구", "종로구", "구로구", "강서구"];
    const filteredData = data.filter((item) =>
      targetDistricts.some((district) => item.ADDR.includes(district))
    );

    if (filteredData.length > 0) {
      console.log(`Pod ${POD_NAME}: 필터링된 데이터 개수 ${filteredData.length}`);
      await sendToKafka("parking-topic", {
        podName: POD_NAME,
        startIndex,
        endIndex,
        data: filteredData,
      }); // Kafka로 전송
    }
  } catch (error) {
    console.error(`Pod ${POD_NAME}: 작업 처리 중 오류 발생: ${error.message}`);
  }
}

// Express 서버 시작
async function startServer() {
  try {
    await producer.connect(); // Kafka 프로듀서 연결
    console.log("Kafka 프로듀서 연결 성공");

    processWork(); // 작업 처리 시작

    app.listen(PORT, () => {
      console.log(`서버 실행 중: http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Kafka 프로듀서 연결 실패:", error.message);
    process.exit(1);
  }
}

// 간단한 상태 확인용 엔드포인트
app.get("/", (req, res) => {
  res.status(200).send("OK");
});

startServer();
