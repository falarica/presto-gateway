# Presto Gateway Web UI 
The Presto Gateway Web UI is composed of several Angular components and is written in typescript . User have to build the UI as per following steps given below which builds UI code and copy it to "steerd-gateway/presto-gateway-main/src/main/resources/gatewaywebapp" folder . Then have to do maven build and docker build images as per given in Presto Gateway documentation .

## What's Included
- Angular 9 & Typescript
- Bootstrap 4+ & SCSS
- Responsive layout

## Prerequisite
- node - v12.18.3
- npm - 6.14.8

## Steps to install node & npm
- curl https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash
- export NVM_DIR="$HOME/.nvm"
  [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
  [ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"
- nvm install v12.18.3  # This installs node and npm 


## Build

- npm install --prefix steerd-gateway/presto-gateway-main/src/main/ngapp/
- npm run-script build --prefix steerd-gateway/presto-gateway-main/src/main/ngapp/

