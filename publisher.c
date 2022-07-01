#include <stdio.h>
#include <stdlib.h>
#include <netdb.h>
#include <string.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#define TOPIC_NAME_LEN 50
#define MESSAGE_SIZE 512
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

struct TOPIC{         								//structure for the topic stored in topic.bin file
	int id;
	char name[TOPIC_NAME_LEN+1];
}; 

struct MESSAGE{
	char message[MESSAGE_SIZE+1];
	struct TOPIC topic;
};

int sd;

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

void create_topic(){                         //creating a new topic
	char topic[TOPIC_NAME_LEN+1];
	while(1){
		bzero(topic,TOPIC_NAME_LEN+1);
		printf("enter the new topic(maximum possible length is 50): ");
		scanf("%s",topic);
		getchar();
		int len = strlen(topic);
		if(len > TOPIC_NAME_LEN){
			printf("topic name exceeded limit\n");
			continue;
		}
		if(search_topic(topic)){
			printf("topic with same name already present\n");
			continue;
		}
		break;
	}
	FILE *fp = fopen("topic.bin","w+");
	int count;
	fread(&count, sizeof(int),1,fp);
	fseek(fp,0,SEEK_END);
	struct TOPIC tp;
	tp.id = count+1;
	strcpy(tp.name,topic);
	fwrite(&tp, sizeof(tp), 1, fp);
	printf("\t\tnew topic created\n\t\tname= %s\n\t\tid= %d\n",tp.name,tp.id);
	fclose(fp);
}

void send_to_broker(char *message, char *topic){
	char msg[MESSAGE_SIZE+1+TOPIC_NAME_LEN+1+2];
	bzero(msg,sizeof(msg));
	strcpy(msg, message);
	strcat(msg,"\r");
	strcat(msg, topic);
	strcat(msg, "\r");
	printf("\t\tmessage sent\n\t\tmessage= %s\n",msg);
	send(sd, msg, sizeof(msg),0);
}

void send_message(){
	char message[MESSAGE_SIZE+1];
	while(1){
		bzero(message,MESSAGE_SIZE+1);
		printf("enter your message:\n");
		scanf("%[^\n]",message);
		getchar();
		int len = strlen(message);
		if(len > MESSAGE_SIZE){
			printf("message exceeded the limit\n");
			continue;
		}
		break;
	}
	
	char topic[TOPIC_NAME_LEN+1];
	while(1){
		bzero(topic,TOPIC_NAME_LEN+1);
		printf("enter topic of message:\n");
		scanf("%[^\n]",topic);
		getchar();
		int len = strlen(topic);
		if(len > TOPIC_NAME_LEN){
			printf("topic name exceeded limit\n");
			continue;
		}
		if(!search_topic(topic)){
			printf("specified topic does not exist\n");
			continue;
		}
		break;
	}
	send_to_broker(message, topic);
	
}

void send_file_data(){
	printf("enter the path or file name to send: ");
	char path[200];
	scanf("%s",path);
	getchar();
	FILE *fp = fopen(path, "r");
	char topic[TOPIC_NAME_LEN+1];
	while(1){
		bzero(topic,TOPIC_NAME_LEN+1);
		printf("enter topic of message:\n");
		scanf("%[^\n]",topic);
		getchar();
		int len = strlen(topic);
		if(len > TOPIC_NAME_LEN){
			printf("topic name exceeded limit\n");
			continue;
		}
		if(!search_topic(topic)){
			printf("specified topic does not exist\n");
			continue;
		}
		//printf("right topic is given\n");
		break;
	}
	//printf("now ready to read file\n");
	char message[MESSAGE_SIZE+1];
	char c;
	int index;
	while(!feof(fp)){
		index = 0; 
		bzero(message,sizeof(message));
		for(int i = 0; !feof(fp) && i<MESSAGE_SIZE; i++){	
			c = fgetc(fp);
			message[index] = c;
			index++;
		}
		message[index] = '\0';
		//printf("\t\tmessage length: %lu\n\t\ttopic length: %lu\n",strlen(message),strlen(topic));
		//printf("\t\tmessage: %s\n\t\ttopic: %s\n",message,topic);
		send_to_broker(message, topic);
	}
	fclose(fp);
	
}

int main(int argc,char **argv)
{
	struct	sockaddr_in server;
	
	sd = socket (AF_INET,SOCK_STREAM,IPPROTO_TCP);
	if(sd < 0){
		perror("socket");
		exit(0);
	}

	server.sin_family = AF_INET;
	server.sin_addr.s_addr=inet_addr(argv[1]);
	//inet_aton(argv[1],&server.sin_addr);
	//inet_pton(AF_INET, argv[1], &server.sin_addr);
	server.sin_port = htons(12345);

	if(connect(sd, (struct sockaddr*) &server, sizeof(server)) == -1){
		perror("connect");
		exit(0);
	}

    for(;;) {
	   printf("\t\tcreate a topic: n\n\t\tsending a message: s\n\t\ttaking a file and send: f\n\t\tquit: q\n");
	   char c;
	   scanf("%c",&c);
	   getchar();
	   switch(c){
	   		case 'n':
	   			create_topic();
	   			break;
	   		case 's':
	   			send_message();
	   			break;
	   		case 'f':
	   			send_file_data();
	   			break;
	   		case 'q':
	   			close(sd);
	   			printf("q is typed\n");
	   			exit(0);
	   		default:
	   			printf("please choose the correct option\n");
	   			break;
	   }
	 
    }
	close(sd);
}
