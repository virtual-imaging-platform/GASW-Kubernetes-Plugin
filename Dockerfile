# This image contain ; podman and singularity runnable as non-root user.
# For this case it's the podman user with the UID/GID 48
# We allow 65536 namespaces to this user as recommended by the singularity doc

FROM quay.io/podman/stable:v5.2.2-immutable

ADD dummy.sh /home/podman/dummy.sh
RUN chmod +x /home/podman/dummy.sh

RUN python3 -m ensurepip
RUN pip3 install boutiques==0.5.26
RUN dnf install git net-tools libselinux-utils which procps jq time cpio curl apptainer -y

# We change to the apache ID user/group to be able to manage files in workflow-xxxxx/ (owned by an apache user)
RUN usermod -u 48 podman
RUN groupmod -g 48 podman

# We also need to edit the number of linux usernamespace for the user (also group)
# see : https://www.redhat.com/sysadmin/7-linux-namespaces
# also : https://en.wikipedia.org/wiki/Linux_namespaces
# podman:100000:65536 should be read like that :
# podman has 65536 namespaces availables starting from the 100000 namespaces
# We start from a big number to avoid overlap. 
RUN sudo sed -i 's/podman:1:999/podman:100000:65536/' /etc/subuid
RUN sudo sed -i 's/podman:1:999/podman:100000:65536/' /etc/subgid

RUN mkdir -p /var/www/html/workflows/

# Command were runned as root so now we pass on podman user (to make it rootless)
USER 48

ENV PATH="/home/podman/.local/bin:${PATH}"
RUN mkdir -p /home/podman/.local/bin
RUN ln -s /home/podman/dummy.sh /home/podman/.local/bin/docker