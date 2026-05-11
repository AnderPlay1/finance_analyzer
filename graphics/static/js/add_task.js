function auto_height(elem) {
  // Сбрасываем высоту, чтобы она соответствовала текущему объему текста
  elem.style.height = '1px';
  elem.style.height = `${elem.scrollHeight}px`;
}