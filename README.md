Prerequesits:
- Node.js >= 16.x [https://nodejs.org/en/download/]

After cloning the repository, create a file called .env with two environment variables in it.
```
DB_USER=postgres
DB_PASSWORD=setpassword
```

Change the user and password to match your database configuration.


Run the following command to load all dependencies:
```
npm install
```

Start the data-generator:
```
node publish.js
```

Start the subscriber:
```
node subscribe.js
```