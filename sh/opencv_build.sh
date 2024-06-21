# https://ahelpme.com/linux/ubuntu/install-and-make-gnu-gcc-10-default-in-ubuntu-20-04-focal/

version=$1
[ $version ] || version=4.8.0

prefix=$1
[ $prefix ] || prefix=/data/package/env/3.9

cd opencv

sudo rm -rf build
mkdir build
cd build

cmake -D CMAKE_BUILD_TYPE=RELEASE \
      -D CMAKE_INSTALL_PREFIX=$prefix \
      -D CMAKE_CUDA_STANDARD=“11” \
      -D INSTALL_C_EXAMPLES=ON \
      -D INSTALL_PYTHON_EXAMPLES=ON \
      -D ENABLE_FAST_MATH=1 \
      -D USE_CUDA=1 \
      -D CUDA_FAST_MATH=1 \
      -D WITH_CUBLAS=1 \
      -D WITH_CUDA=ON \
      -D BUILD_opencv_cudacodec=OFF \
      -D WITH_CUDNN=ON \
      -D OPENCV_DNN_CUDA=ON \
      -D CUDA_ARCH_BIN=7.5 \
      -D WITH_TBB=ON \
      -D WITH_V4L=ON \
      -D WITH_QT=ON \
      -D WITH_OPENGL=ON \
      -D OPENCV_EXTRA_MODULES_PATH=../../opencv_contrib/modules \
      -D BUILD_EXAMPLES=OFF ..

make -j4
sudo sh opencv_install.sh $prefix
