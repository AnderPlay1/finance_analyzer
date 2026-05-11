document.addEventListener('DOMContentLoaded', function() {
    const urlParams = new URLSearchParams(window.location.search);
    const section = urlParams.get('section');
    if (section) {
        const sectionElement = document.getElementById(section);
        if (sectionElement) {
            sectionElement.classList.add('active');
        }
        const linkElement = document.getElementById('link-' + section);
        if (linkElement) {
            linkElement.classList.add('active');
        }
    }
});

function copyText() {
    navigator.clipboard.writeText("{{ course_id }}").then(function() {
        var tooltip = document.getElementById('custom-tooltip');
        var button = document.querySelector('.course-id');
        var rect = button.getBoundingClientRect();
        tooltip.style.top = (rect.top - 5) + 'px';
        tooltip.style.left = (rect.left) + 'px';
        tooltip.style.display = 'block';
        tooltip.style.position = 'fixed';
        setTimeout(function() {
            tooltip.style.display = 'none';
        }, 1000);
    });
}
document.getElementById('add-user-btn').addEventListener('click', function() {
    var userForm = document.getElementById('add-user-form');
    var optionForm = document.getElementById('add-option-form');
    if (userForm.style.display === 'none') {
        userForm.style.display = 'block';
        optionForm.style.display = 'none';
    } else {
        userForm.style.display = 'none';
        document.getElementById('user-form').reset();
    }
});

document.getElementById('add-option-btn').addEventListener('click', function() {
    var optionForm = document.getElementById('add-option-form');
    var userForm = document.getElementById('add-user-form');
    if (optionForm.style.display === 'none') {
        optionForm.style.display = 'block';
        userForm.style.display = 'none';
    } else {
        optionForm.style.display = 'none';
        document.getElementById('option-form').reset();
    }
});

function showPopup(message) {
    var popup = document.getElementById('popup-message');
    popup.textContent = message;
    popup.style.display = 'block';
    setTimeout(function() {
        popup.style.display = 'none';
    }, 2000);
}

document.getElementById('user-form').addEventListener('submit', function(event) {
    event.preventDefault();
    showPopup('Отправлено!');
    document.getElementById('add-user-form').style.display = 'none';
    this.submit(); // Submit the form after showing the popup
    document.getElementById('user-form').reset();
});

document.getElementById('option-form').addEventListener('submit', function(event) {
    event.preventDefault();
    showPopup('Отправлено!');
    document.getElementById('add-option-form').style.display = 'none';
    this.submit(); // Submit the form after showing the popup
    document.getElementById('option-form').reset();
});