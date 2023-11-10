
Grafana Enterprise was installed, with apt, as per the official instruction here:

https://grafana.com/docs/grafana/latest/setup-grafana/installation/debian/


# Add repository
sudo apt-get install -y apt-transport-https software-properties-common wget


sudo mkdir -p /etc/apt/keyrings/

wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null


echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee -a /etc/apt/sources.list.d/grafana.list


# Updates the list of available packages
sudo apt-get update


# Installs the latest Enterprise release:
sudo apt-get install grafana-enterprise


# Create and start service
sudo systemctl daemon-reload

sudo systemctl start grafana-server

sudo systemctl status grafana-server


sudo systemctl enable grafana-server.service



# Grafana Configuration
The grafana.ini file located here is up-to-date, at the time of writing.
And may therefore be used when recreating the setup.
grafana.ini should be placed in /etc/grafana/grafana.ini