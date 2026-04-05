## vk-tarantool-project

**_Запуск через Docker Compose_**  
docker-compose up -d

**_Сборка проекта_**  
mvn clean compile

**_Запуск сервера_**  
mvn exec:java -Dexec.mainClass="ru.fisher.tarantool.TarantoolServer"

**_Запуск тестов_**  
mvn exec:java -Dexec.mainClass="ru.fisher.tarantool.TestClient"
