Prerequesits:
- Node.js >= v16 [https://nodejs.org/en/download/]

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

Start the etl-script:
```
node etl.js
```