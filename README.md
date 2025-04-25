# Webhook Application

A Spring Boot application that automatically interacts with a remote API to solve the mutual followers problem.

## Problem Statement
The application identifies mutual follow pairs where both users follow each other, outputting only direct 2-node cycles as [min, max] once.

## Features
- Automatic API interaction on startup
- JWT authentication
- Retry mechanism for failed requests
- Efficient mutual followers algorithm

## Build Instructions

### Prerequisites
- Java 17 or higher
- Maven 3.6 or higher

### Building the Application
1. Clone the repository
2. Navigate to the project directory
3. Run the following command to build the JAR:
```bash
mvn clean package
```

The executable JAR will be created in the `target` directory with the name `webhook-app-0.0.1-SNAPSHOT.jar`.

### Running the Application
```bash
java -jar target/webhook-app-0.0.1-SNAPSHOT.jar
```

## Project Structure
- `src/main/java/com/example/webhookapp/`: Main application code
- `src/main/resources/`: Configuration files
- `src/test/`: Test files

## API Endpoints
- POST `/hiring/generateWebhook`: Initial request to get webhook URL and access token
- GET `/users`: Get users data
- POST `/hiring/testWebhook`: Submit solution

## Retry Policy
- The application will retry failed requests up to 4 times
- Exponential backoff is implemented with a base delay of 1 second

## JAR Download
You can download the latest JAR from the [Releases](https://github.com/yourusername/bajaj-finserv-challenge/releases) page.

## License
MIT License 