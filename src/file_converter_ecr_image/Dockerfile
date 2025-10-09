FROM public.ecr.aws/shelf/lambda-libreoffice-base:7.6-python3.12-x86_64

# Instalar fuentes comunes (Microsoft y otras)
# Instalar fuentes comunes en Amazon Linux 2023
RUN dnf install -y \
      fontconfig \
      dejavu-sans-fonts \
      dejavu-serif-fonts \
      dejavu-sans-mono-fonts \
      liberation-fonts \
      google-noto-sans-fonts \
      google-noto-serif-fonts && \
    dnf clean all

# Asegurar varias version de libreoffice
RUN ln -s /usr/bin/libreoffice7.6 /usr/bin/libreoffice && \
    ln -s /usr/bin/libreoffice7.6 /usr/bin/soffice

RUN fc-cache -f -v

COPY index.py ${LAMBDA_TASK_ROOT}/

CMD [ "index.lambda_handler" ]