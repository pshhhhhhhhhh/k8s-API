import express from "express";
import axios from "axios";
import fs from "fs";
import os from "os";
import { Kafka, Partitioners } from "kafkajs";
import https from "https";

const app = express();
const PORT = process.env.PORT || 3000;

const API_KEY = process.env.API_KEY || "발급받은 API키";
const BASE_URL = "http://openapi.seoul.go.kr:8088";

const kafkaBrokers = process.env.KAFKA_BROKERS || "10.10.150.124:9092";
const kafka = new Kafka({
  clientId: "parking-api-client",
  brokers: kafkaBrokers.split(","),
});
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const POD_NAME = process.env.POD_NAME || os.hostname();

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
      .sort((a, b) => a.metadata.name.localeCompare(b.metadata.name)); // 이름으로 정렬

    const currentPodIndex = pods.findIndex(
      (pod) => pod.metadata.name === POD_NAME
    );

    console.log(`Pod List: ${pods.map((pod) => pod.metadata.name)}`);
    console.log(`Current Pod: ${POD_NAME}, Index: ${currentPodIndex}`);

    return {
      podCount: pods.length,
      podIndex: currentPodIndex >= 0 ? currentPodIndex : 0,
    };
  } catch (error) {
    console.error("Error fetching Pod details:", error.message);
    return { podCount: 1, podIndex: 0 }; // 기본값 반환
  }
}

async function fetchTotalDataCount() {
  const url = `${BASE_URL}/${API_KEY}/json/GetParkingInfo/1/1`;
  try {
    const response = await axios.get(url);
    if (response.data.GetParkingInfo?.RESULT?.CODE !== "INFO-000") {
      throw new Error(`OpenAPI 오류: ${response.data.GetParkingInfo?.RESULT?.MESSAGE}`);
    }
    return response.data.GetParkingInfo?.list_total_count || 0;
  } catch (error) {
    console.error(`OpenAPI 데이터 크기 조회 실패: ${error.message}`);
    throw error;
  }
}

async function fetchParkingData(startIndex, endIndex) {
  const MAX_BATCH_SIZE = 1000; // OpenAPI 최대 요청 크기
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

      allData = allData.concat(response.data.GetParkingInfo?.row || []);
    } catch (error) {
      console.error(
        `OpenAPI 호출 실패 (범위 ${batchStart} ~ ${batchEnd}): ${error.message}`
      );
      throw error;
    }
  }

  return allData;
}

async function sendToKafka(topic, data) {
  const key = `${POD_NAME}-${data.startIndex}-${data.endIndex}`;
  try {
    await producer.send({
      topic,
      messages: [{ key, value: JSON.stringify(data) }],
    });
    console.log(`Kafka 전송 성공: key=${key}, 데이터 개수=${data.data.length}`);
  } catch (error) {
    console.error(`Kafka 메시지 전송 실패: ${error.message}`);
  }
}

function calculateRange(totalData, podIndex, podCount) {
  const rangeSize = Math.ceil(totalData / podCount);
  const startIndex = podIndex * rangeSize + 1;
  const endIndex = Math.min((podIndex + 1) * rangeSize, totalData);
  console.log(`Pod ${POD_NAME} 범위: ${startIndex} ~ ${endIndex}`);
  return { startIndex, endIndex };
}

async function processWork() {
  try {
    const { podCount, podIndex } = await getPodDetails();
    const totalData = await fetchTotalDataCount();
    console.log(`총 데이터 크기: ${totalData}, Pod Count: ${podCount}`);

    const { startIndex, endIndex } = calculateRange(totalData, podIndex, podCount);
    console.log(`Pod ${POD_NAME}: 처리 범위 ${startIndex} ~ ${endIndex}`);

    const data = await fetchParkingData(startIndex, endIndex);

    const targetDistricts = ["중구", "관악구"];
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
      });
    }
  } catch (error) {
    console.error(`Pod ${POD_NAME}: 작업 처리 중 오류 발생: ${error.message}`);
  }
}

async function startServer() {
  try {
    await producer.connect();
    console.log("Kafka 프로듀서 연결 성공");

    processWork();

    app.listen(PORT, () => {
      console.log(`서버 실행 중: http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error("Kafka 프로듀서 연결 실패:", error.message);
    process.exit(1);
  }
}

app.get("/", (req, res) => {
  res.status(200).send("OK");
});

startServer();
