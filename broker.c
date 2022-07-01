#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#include <sys/sem.h>
#include <pthread.h>
#define TOPIC_NAME_LEN 50
#define MESSAGE_SIZE 512
#define MAX_CLIENT 100

int id, id1;
int semid;
struct sembuf operations[2];
pthread_mutex_t counter_mutex = PTHREAD_MUTEX_INITIALIZER;
struct TOPIC{         								//structure for the topic stored in topic.bin file
	int id;
	char name[TOPIC_NAME_LEN+1];
}; 

struct MESSAGE{
	char message[MESSAGE_SIZE+1];
	struct TOPIC topic;
};

int search_topic(char *topic){   						//check for the availability of topic already
	FILE *fp = fopen("topic.bin","r");
	struct TOPIC tp;
	while(!feof(fp)){
		fread(&tp, sizeof(tp), 1, fp);
		if(!strcmp(tp.name, topic)){
			fclose(fp);
			return tp.id;
		}
	}
	fclose(fp);
	return 0;
}

char* get_from_message_queue(){
	struct MESSAGE msg;
	
	int retval = semop(semid, operations, 1);
	if(retval == 0){
		FILE *fp = fopen("message_queue.bin","w");
		fseek(fp,(-1*sizeof(struct MESSAGE)),SEEK_END);
		fread(&msg, sizeof(msg),1,fp);
		fclose(fp);	
	}
	semop(semid, operations, 0);
	char *buffer = (char*)malloc(sizeof(char)*(MESSAGE_SIZE+1+TOPIC_NAME_LEN+1+2));
	bzero(buffer,sizeof(buffer));
	strcpy(buffer, msg.message);
	strcat(buffer,"\r");
	strcat(buffer, msg.topic.name);
	strcat(buffer, "\r");
	return buffer;
}

void put_into_message_queue(char *buffer){
	int len = strlen(buffer);
	char c;
	char message[MESSAGE_SIZE+1];
	char topic[TOPIC_NAME_LEN+1];
	int index1 = 0;
	int i = 0;
	while(i<len){
		c = buffer[i];
		if(c == '\r'){
			message[index1] = '\0';
			i++;
			break;
		}
		message[index1] = c;
		index1++;
		i++;
	}
	index1 = 0;
	while(i<len){
		c = buffer[i];
		if(c == '\r'){
			topic[index1] = '\0';
			i++;
			break;
		}
		topic[index1] = c;
		index1++;
		i++;
	}
	printf("\t\tmessage length: %lu\n\t\ttopic length: %lu\n",strlen(message),strlen(topic));
	printf("\t\tmessage received: %s\n\t\ttopic: %s\n",message,topic);
	struct MESSAGE msg;
	strcpy(msg.message, message);
	strcpy(msg.topic.name,topic);
	msg.topic.id = search_topic(topic);
	int retval = semop(semid, operations, 1);
	if(retval == 0){
		FILE *fp = fopen("message_queue.bin","w");
		fseek(fp,0,SEEK_END);
		fwrite(&msg, sizeof(msg),1,fp);
		fclose(fp);	
	}
	semop(semid, operations, 0);
	
}

void *publisher_work(void* fd_addr){
	int fd = *((int*)fd_addr);	
	int msglen = MESSAGE_SIZE+1+TOPIC_NAME_LEN+1+2;
	char buffer[msglen+1];
	int lastIndex;
	while(1){
		bzero(&buffer, sizeof(buffer));
		if((lastIndex = recv(fd, buffer, msglen+1, 0)) < 0){
		       perror("recv");
		       exit(0);
		}
		if(lastIndex == 0){
			printf("client has closed the connection\n");
			close(fd);
			pthread_exit(0);
		}
		buffer[lastIndex] = '\0';
		printf("\t\t\t%c\n",buffer[0]);
		put_into_message_queue(buffer);
	}
}

void* talk_with_publisher(){
	id = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	printf("socket is created\n");
	struct sockaddr_in serveraddr, clientaddr;
	memset(&serveraddr, 0 , sizeof(serveraddr));
	memset(&clientaddr, 0 , sizeof(clientaddr));
	printf("memory is setted\n");
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(12345);
	serveraddr.sin_addr.s_addr = inet_addr("10.0.2.15");
	printf("address is assigned to server\n");
	if(bind(id, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) < 0){
		perror("bind");
	}
	printf("address is bounded to server\n");
	printf("now server is listening\n");
	if(listen(id, 100) < 0){
		perror("listen");
	}
	printf("now server is accepting\n");
	unsigned int clientlen;
	clientlen = sizeof(clientaddr);
	int fd;
	int client_count = 0;
	pthread_t tid[MAX_CLIENT];
	while(1){
		if((fd = accept(id, (struct sockaddr*)&clientaddr, &clientlen))< 0){
			perror("accept");
		}
		printf("connection is accepted with client: %s, %d\n", inet_ntoa(clientaddr.sin_addr), ntohs(clientaddr.sin_port));
		if(client_count == MAX_CLIENT){
			printf("sorry more publishers can't be joined now\n");
			char message[50];
			strcpy(message, "sorry more publishers can't be joined now");
			send(fd,message, sizeof(message), 0); 
			close(fd);
			continue;
		} 
		pthread_create(&tid[client_count++], NULL, &publisher_work, &fd); 
	}
	
	
}

void *subscriber_work(void* fd_addr){
	int fd = *((int*)fd_addr);
	int msglen = MESSAGE_SIZE+1+TOPIC_NAME_LEN+1+2;
	char choice[10];
	int lastIndex;
	while(1){
		bzero(choice, sizeof(choice));
		if((lastIndex = recv(fd, choice, sizeof(choice), 0)) < 0){
		       perror("recv");
		       exit(0);
		}
		if(lastIndex == 0){
			printf("client has closed the connection\n");
			close(fd);
			pthread_exit(0);
		}
		choice[lastIndex] = '\0';
		int ch = atoi(choice);
		if(ch == 1){
			char *buffer;
			buffer = get_from_message_queue();	
			send(fd, buffer, sizeof(buffer), 0);
		}
	}
}

void* talk_with_subscriber(){
	id1 = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	printf("socket is created\n");
	struct sockaddr_in serveraddr, clientaddr;
	memset(&serveraddr, 0 , sizeof(serveraddr));
	memset(&clientaddr, 0 , sizeof(clientaddr));
	printf("memory is setted\n");
	serveraddr.sin_family = AF_INET;
	serveraddr.sin_port = htons(12346);
	serveraddr.sin_addr.s_addr = inet_addr("10.0.2.15");
	printf("address is assigned to server\n");
	if(bind(id1, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) < 0){
		perror("bind");
	}
	printf("address is bounded to server\n");
	printf("now server is listening\n");
	if(listen(id1, 100) < 0){
		perror("listen");
	}
	printf("now server is accepting\n");
	unsigned int clientlen;
	clientlen = sizeof(clientaddr);
	int fd;
	int client_count = 0;
	pthread_t tid[MAX_CLIENT];
	while(1){
		if((fd = accept(id1, (struct sockaddr*)&clientaddr, &clientlen))< 0){
			perror("accept");
		}
		printf("connection is accepted with client: %s, %d\n", inet_ntoa(clientaddr.sin_addr), ntohs(clientaddr.sin_port));
		if(client_count == MAX_CLIENT){
			printf("sorry more subscribers can't be joined now\n");
			char message[50];
			strcpy(message, "sorry more subscribers can't be joined now");
			send(fd,message, sizeof(message), 0); 
			close(fd);
			continue;
		} 
		pthread_create(&tid[client_count++], NULL, &subscriber_work, &fd); 
	}
}

int main(int argc, char *args[]){
	semid = semget(IPC_PRIVATE, 1, IPC_CREAT | 0666);        //value 1 means free to use and value 0 means resource is busy file
	//starting from 1
	semctl(semid, 0, SETVAL, 1);
	
	operations[0].sem_num = 0;
	operations[0].sem_op = 1;
	operations[0].sem_flg = 0;
	operations[1].sem_num = 0;
	operations[1].sem_op = -1;
	operations[1].sem_flg = 0;
	pthread_t tid_publisher, tid_subscriber;
	pthread_create(&tid_publisher, NULL, &talk_with_publisher, NULL);
	pthread_create(&tid_subscriber, NULL, &talk_with_subscriber, NULL);
	pthread_join(tid_publisher,NULL);
	pthread_join(tid_subscriber, NULL);
	exit(0);
	
	
}
/*
P2.
-In this problem let us extend Message Queues network wide for the following characteristics.
One who writes a message is called a publisher and one who reads is called as subscriber. A
publisher tags a message with a topic. Anyone who subscribed to that topic can read that
message. There can be many subscribers and publishers for a topic but there can only be one
publisher for a given message.  
sol: every new tag(topic) is given a unique long int number. 

-Publisher program should provide an interface for the user to (i) create a topic. Publisher
also provides commands for (ii) sending a message, (iii) taking a file and send it as a series
of messages. When sending a message, topic must be specified. Each message can be up to
512 bytes.

-Publisher program takes address of a Broker server as CLA. There can be several broker
servers on separate machines or on a single machine. The role of a broker server is to receive
messages from a publisher and store them on disk and send messages to a subscriber when
requested,

-Publishers and subscribers may be connected to different brokers. The messages should reach
the right subscriber.

-Subscriber program takes the address of a broker server as CLA at the startup. It allows a
user to (i) subscribe to a topic (ii) retrieve next message (iii) retrieve continuously all
messages. Subscriber should print the message id, and the message.

-All brokers are connected in a circular topology. For message routing, the broker connected
to a subscriber, queries its neighbor brokers and they query further and so on. Each query
retrieves a bulk of messages limited by BULK_LIMIT (default=10).

Brokers store messages for a period of MESSAGE_TIME_LIMIT (default=1minute)
This system doesn’t guarantee FIFO order of messages. Think and propose any mechanism
that can guarantee FIFO order.
*/
